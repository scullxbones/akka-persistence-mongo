/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import play.api.libs.iteratee._
import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands._
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.bson._
import reactivemongo.core.nodeset.Authenticate

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object RxMongoPersistenceDriver {
  import MongoPersistenceDriver._

  def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean): WriteConcern = (writeSafety, wtimeout.toMillis.toInt, fsync) match {
    case (Unacknowledged, wt, f) =>
      WriteConcern.Unacknowledged.copy(fsync = f, wtimeout = Option(wt))
    case (Acknowledged, wt, f) =>
      WriteConcern.Acknowledged.copy(fsync = f, wtimeout = Option(wt))
    case (Journaled, wt, _) =>
      WriteConcern.Journaled.copy(wtimeout = Option(wt))
    case (ReplicaAcknowledged, wt, f) =>
      WriteConcern.ReplicaAcknowledged(2, wt, !f)
  }
}

class RxMongoDriverProvider(actorSystem: ActorSystem) {
  val driver: MongoDriver = {
    val md = MongoDriver()
    actorSystem.registerOnTermination(driver.close())
    md
  }
}

class RxMongoDriver(system: ActorSystem, config: Config, driverProvider: RxMongoDriverProvider) extends MongoPersistenceDriver(system, config) {
  import RxMongoPersistenceDriver._

  val RxMongoSerializers: RxMongoSerializers = RxMongoSerializersExtension(system)

  // Collection type
  type C = Future[BSONCollection]

  type D = BSONDocument

  private def rxSettings = RxMongoDriverSettings(system.settings)
  private[mongodb] val driver = driverProvider.driver
  private[this] lazy val parsedMongoUri = MongoConnection.parseURI(mongoUri) match {
    case Success(parsed)    => parsed
    case Failure(throwable) => throw throwable
  }

  implicit val waitFor: FiniteDuration = 10.seconds

  private[this] lazy val unauthenticatedConnection: MongoConnection = wait {
    // create unauthenticated connection, there is no direct way to wait for authentication this way
    // plus prevent sending double authentication (initial authenticate and our explicit authenticate)
    driver.connection(parsedURI = parsedMongoUri.copy(authenticate = None))
      .database(name = dbName, failoverStrategy = failoverStrategy)(system.dispatcher)
      .map(_.connection)(system.dispatcher)
  }

  private[mongodb] lazy val connection: MongoConnection =
    // now authenticate explicitly and wait for confirmation
    parsedMongoUri.authenticate.fold(unauthenticatedConnection) { auth =>
      waitForAuthentication(unauthenticatedConnection, auth)
    }

  private[this] def waitForAuthentication(conn: MongoConnection, auth: Authenticate): MongoConnection = {
    wait(conn.authenticate(auth.db, auth.user, auth.password))
    conn
  }
  private[this] def wait[T](awaitable: Awaitable[T])(implicit duration: Duration): T =
    Await.result(awaitable, duration)

  def walk(collection: Future[BSONCollection])(previous: Seq[WriteResult], doc: BSONDocument)(implicit ec: ExecutionContext): Future[Seq[WriteResult]] = {
    import DefaultBSONHandlers._
    import Producer._
    import RxMongoSerializers._

    import scala.collection.immutable.{Seq => ISeq}

    val id = doc.getAs[BSONObjectID]("_id").get
    val ev = Event[BSONDocument](useLegacySerialization)(deserializeJournal(doc).toRepr)
    val q = BSONDocument("_id" -> id)

    val atom = serializeJournal(Atom(ev.pid, ev.sn, ev.sn, ISeq(ev)))
    val results = collection.flatMap(_.update(q, atom, journalWriteConcern, upsert = false, multi = false).map { wr =>
      previous :+ wr
    })
    results.onComplete {
      case Success(s) => logger.debug(s"update completed ... ${s.size - 1} so far")
      case Failure(t) => logger.error(s"update failure", t)
    }
    results
  }

  override private[mongodb] def upgradeJournalIfNeeded(): Unit = upgradeJournalIfNeeded("")

  override private[mongodb] def upgradeJournalIfNeeded(persistenceId: String): Unit = {
    import JournallingFieldNames._

    import scala.concurrent.ExecutionContext.Implicits.global

    val j = getJournal(persistenceId)
    val walker = walk(j) _
    val q = BSONDocument(VERSION -> BSONDocument("$exists" -> 0))
    val empty: Seq[WriteResult] = DefaultWriteResult(
      ok = true, n = 0,
      writeErrors = Seq.empty, writeConcernError = None,
      code = None, errmsg = None) :: Nil

    def traverse(count: Int): Future[Seq[WriteResult]] = {
      logger.info(s"Journal automatic upgrade found $count records needing upgrade")
      if (count > 0) {
        j.flatMap(_.find(q)
                    .cursor[BSONDocument]()
                    .enumerate()
                    .run(Iteratee.foldM(empty)(walker)))
      } else Future.successful(empty)
    }

    val eventuallyUpgrade = for {
      journal <- j
      _ <- journal.remove(BSONDocument(PROCESSOR_ID -> BSONRegex("^/user/sharding/[^/]+Coordinator/singleton/coordinator", "")))
        .map(wr => logger.info(s"Successfully removed ${wr.n} legacy cluster sharding records"))
        .recover { case t => logger.error(s"Error while removing legacy cluster sharding records", t) }
      indices <- journal.indexesManager.list()
      _ <- indices
        .find(_.key.sortBy(_._1) == Seq(DELETED -> IndexType.Ascending, PROCESSOR_ID -> IndexType.Ascending, SEQUENCE_NUMBER -> IndexType.Ascending))
        .map(_.eventualName)
        .map(n => journal.indexesManager.drop(n).transform(
          _ => logger.info("Successfully dropped legacy index"),
          { t =>
            logger.error("Error received while dropping legacy index", t)
            t
          }))
        .getOrElse(Future.successful(()))
      count <- journal.count(Option(q))
      wr <- traverse(count)
    } yield wr

    eventuallyUpgrade.onComplete {
      case Success(wrs) if wrs.exists(w => w.writeErrors.nonEmpty || w.writeConcernError.nonEmpty) =>
        val errors = wrs.flatMap(_.writeConcernError).map(r => s"${r.code} - ${r.errmsg}").mkString("\n")
        logger.error("Upgrade did not complete successfully")
        logger.error(s"Errors during journal auto-upgrade:\n$errors")
        val writeErrors = wrs.flatMap(_.writeErrors).map(we => s"${we.code} - ${we.errmsg}").mkString("\n")
        logger.error(s"Received ${wrs.count(_.writeErrors.nonEmpty)} write errors during journal auto-upgrade:\n$writeErrors")
      case Success(wrs) =>
        val successCount = wrs.foldLeft(0)((sum, wr) => sum + wr.n)
        logger.info(s"Successfully upgraded $successCount records")
      case Failure(t) =>
        logger.error(s"Upgrade did not complete successfully", t)
    }

    logger.debug("Waiting on upgrade to complete...")

    Await.result(eventuallyUpgrade, 2.minutes) // ouch

    ()
  }

  private[mongodb] def closeConnections(): Unit = {
    driver.close(5.seconds)
  }

  private[mongodb] def dbName: String = databaseName.getOrElse(parsedMongoUri.db.getOrElse(DEFAULT_DB_NAME))
  private[mongodb] def failoverStrategy: FailoverStrategy = {
    val rxMSettings = rxSettings
    FailoverStrategy(
      initialDelay = rxMSettings.InitialDelay,
      retries = rxMSettings.Retries,
      delayFactor = rxMSettings.GrowthFunction)
  }
  private[mongodb] def db = connection.database(name = dbName, failoverStrategy = failoverStrategy)(system.dispatcher)

  private[mongodb] override def collection(name: String) = db.map(_[BSONCollection](name))(system.dispatcher)
  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety, snapsWTimeout, snapsFsync)
  private[mongodb] def metadataWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)

  private[mongodb] override def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, keys: (String, Int)*)(implicit ec: ExecutionContext) = { collection =>
    val ky = keys.toSeq.map { case (f, o) => f -> (if (o > 0) IndexType.Ascending else IndexType.Descending) }
    def ensureIndex(c: BSONCollection) = c.indexesManager.ensure(Index(
      key = ky,
      background = true,
      unique = unique,
      sparse = sparse,
      name = Some(indexName)))

    collection.flatMap(c =>
      ensureIndex(c).recoverWith {
        case CommandError.Code(26) => //no collection
          c.create().recover {
            case CommandError.Code(48) => //name exists (race condition)
              ()
          }.flatMap(_ => ensureIndex(c))
      }.map(_ => c))
  }

  override private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext) = {
    collection(name).flatMap { cc =>
      cc.stats().flatMap { s =>
        if (!s.capped) cc.convertToCapped(realtimeCollectionSize, None)
        else Future.successful(())
      }.recoverWith {
        case _ => cc.createCapped(realtimeCollectionSize, None)
      }.map(_ => cc)
    }
  }

  private[mongodb] def getCollections(collectionName: String)(implicit ec: ExecutionContext): Enumerator[BSONCollection] = {
    val fut = for {
      database  <- db
      names     <- database.collectionNames
      list      <- Future.sequence(names.filter(_.startsWith(collectionName)).map(collection))
    } yield Enumerator(list: _*)    
    Enumerator.flatten(fut)
  }

  private[mongodb] def getCollectionsAsFuture(collectionName: String)(implicit ec: ExecutionContext): Future[List[BSONCollection]] = {
    getAllCollectionsAsFuture(Option(_.startsWith(collectionName)))
  }

  private[mongodb] def getJournalCollections()(implicit ec: ExecutionContext) = getCollections(journalCollectionName)

  private[mongodb] def getAllCollectionsAsFuture(nameFilter: Option[String => Boolean])(implicit ec: ExecutionContext): Future[List[BSONCollection]] = {
    for {
      database  <- db
      names     <- database.collectionNames
      list      <- Future.sequence(names.filterNot(_ == realtimeCollectionName).filter(nameFilter.getOrElse(_ => true)).map(collection))
    } yield list
  }

  private[mongodb] def journalCollectionsAsFuture(implicit ec: ExecutionContext) = getCollectionsAsFuture(journalCollectionName)
  
  private[mongodb] def getSnapshotCollections()(implicit ec: ExecutionContext) = getCollections(snapsCollectionName)
  
}

class RxMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension {

  val driverProvider: RxMongoDriverProvider = new RxMongoDriverProvider(actorSystem)

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    lazy val driver = new RxMongoDriver(actorSystem, config, driverProvider)

    override lazy val journaler = new RxMongoJournaller(driver) with MongoPersistenceJournalMetrics {
      override def driverName = "rxmongo"
    }

    override lazy val snapshotter = new RxMongoSnapshotter(driver)
    override lazy val readJournal = new RxMongoReadJournaller(driver, ActorMaterializer()(actorSystem))
  }

}

object RxMongoDriverSettings {
  def apply(systemSettings: ActorSystem.Settings): RxMongoDriverSettings = {
    val fullName = s"${getClass.getPackage.getName}.rxmongo"
    val systemConfig = systemSettings.config
    systemConfig.checkValid(ConfigFactory.defaultReference(), fullName)
    new RxMongoDriverSettings(systemConfig.getConfig(fullName))
  }
}

class RxMongoDriverSettings(val config: Config) {

  config.checkValid(config, "failover")

  private val failover = config.getConfig("failover")
  def InitialDelay: FiniteDuration = failover.getFiniteDuration("initialDelay")
  def Retries: Int = failover.getInt("retries")
  def Growth: String = failover.getString("growth")
  def ConstantGrowth: Boolean = Growth == "con"
  def LinearGrowth: Boolean = Growth == "lin"
  def ExponentialGrowth: Boolean = Growth == "exp"
  def Factor: Double = failover.getDouble("factor")

  def GrowthFunction: Int => Double = Growth match {
    case "con" => (_: Int) => Factor
    case "lin" => (i: Int) => i.toDouble
    case "exp" => (i: Int) => math.pow(i.toDouble, Factor)
  }
}