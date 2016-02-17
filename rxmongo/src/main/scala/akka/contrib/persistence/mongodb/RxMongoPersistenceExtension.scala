package akka.contrib.persistence.mongodb

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import play.api.libs.iteratee.Iteratee
import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands._
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.bson._
import reactivemongo.core.nodeset.Authenticate

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Awaitable, ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object RxMongoPersistenceDriver {
  import MongoPersistenceDriver._

  def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean):WriteConcern = (writeSafety,wtimeout.toMillis.toInt,fsync) match {
    case (Unacknowledged,wt,f) =>
      WriteConcern.Unacknowledged.copy(fsync = f, wtimeout = Option(wt))
    case (Acknowledged,wt,f) =>
      WriteConcern.Acknowledged.copy(fsync = f, wtimeout = Option(wt))
    case (Journaled,wt,_) =>
      WriteConcern.Journaled.copy(wtimeout = Option(wt))
    case (ReplicaAcknowledged,wt,f) =>
      WriteConcern.ReplicaAcknowledged(2, wt, !f)
  }
}

class RxMongoDriverProvider {
  val driver = MongoDriver()
}

class RxMongoDriver(system: ActorSystem, config: Config, driverProvider: RxMongoDriverProvider = new RxMongoDriverProvider) extends MongoPersistenceDriver(system, config) {
  import RxMongoPersistenceDriver._

  import concurrent.Await
  import concurrent.duration._

  // Collection type
  type C = BSONCollection

  type D = BSONDocument

  private def rxSettings = RxMongoDriverSettings(system.settings)
  private[mongodb] val driver = driverProvider.driver
  private[this] lazy val parsedMongoUri = MongoConnection.parseURI(mongoUri) match {
    case Success(parsed) => parsed
    case Failure(throwable) => throw throwable
  }
  private[this] lazy val unauthenticatedConnection =
    // create unauthenticated connection, there is no direct way to wait for authentication this way
    // plus prevent sending double authentication (initial authenticate and our explicit authenticate)
    waitForPrimary(driver.connection(parsedURI = parsedMongoUri.copy(authenticate = None)))
  private[mongodb] lazy val connection =
    // now authenticate explicitly and wait for confirmation
    parsedMongoUri.authenticate.fold(unauthenticatedConnection) { auth =>
      waitForAuthentication(unauthenticatedConnection, auth)
    }

  implicit val waitFor = 4.seconds
  private[this] def waitForPrimary(conn: MongoConnection): MongoConnection = {
    wait(conn.waitForPrimary(waitFor minus 1.seconds))
    conn
  }
  private[this] def waitForAuthentication(conn: MongoConnection, auth: Authenticate): MongoConnection = {
    wait(conn.authenticate(auth.db, auth.user, auth.password))
    conn
  }
  private[this] def wait[T](awaitable: Awaitable[T])(implicit duration: Duration): T =
    Await.result(awaitable, duration)

  def walk(collection: BSONCollection)(previous: Seq[WriteResult], doc: BSONDocument)(implicit ec: ExecutionContext): Future[Seq[WriteResult]] = {
    import DefaultBSONHandlers._
    import Producer._
    import RxMongoSerializers._

    import scala.collection.immutable.{Seq => ISeq}

    val id = doc.getAs[BSONObjectID]("_id").get
    val ev = Event[BSONDocument](useLegacySerialization)(deserializeJournal(doc).toRepr)
    val q = BSONDocument("_id" -> id)

    val atom = serializeJournal(Atom(ev.pid, ev.sn, ev.sn, ISeq(ev)))
    val results = collection.update(q, atom, journalWriteConcern, upsert = false, multi = false).map { wr =>
      previous :+ wr
    }
    results.onComplete {
      case Success(s) => logger.debug(s"update completed ... ${s.size - 1} so far")
      case Failure(t) => logger.error(s"update failure",t)
    }
    results
  }

  override private[mongodb] def upgradeJournalIfNeeded(): Unit = {
    import JournallingFieldNames._

    import concurrent.ExecutionContext.Implicits.global

    val j = collection(journalCollectionName)
    val walker = walk(j) _
    val q = BSONDocument(VERSION -> BSONDocument("$exists" -> 0))
    val empty: Seq[WriteResult] = DefaultWriteResult(
      ok = true, n = 0,
      writeErrors = Seq.empty, writeConcernError = None,
      code = None, errmsg = None
    ) :: Nil

    def traverse(count: Int): Future[Seq[WriteResult]] = {
      logger.info(s"Journal automatic upgrade found $count records needing upgrade")
      if (count > 0) {
        j.find(q)
          .cursor[BSONDocument]()
          .enumerate()
          .run(Iteratee.foldM(empty)(walker))
      } else Future.successful(empty)
    }

    val eventuallyUpgrade = for {
      _ <- j.remove(BSONDocument(PROCESSOR_ID -> BSONRegex("^/user/sharding/[^/]+Coordinator/singleton/coordinator","")))
            .map(wr => logger.info(s"Successfully removed ${wr.n} legacy cluster sharding records"))
            .recover { case t => logger.error(s"Error while removing legacy cluster sharding records",t) }
      indices <- j.indexesManager.list()
      _ <- indices
              .find(_.key.sortBy(_._1) == Seq(DELETED -> IndexType.Ascending, PROCESSOR_ID -> IndexType.Ascending, SEQUENCE_NUMBER -> IndexType.Ascending))
              .map(_.eventualName)
              .map(n => j.indexesManager.drop(n).transform(
                _ => logger.info("Successfully dropped legacy index"),
                { t =>
                  logger.error("Error received while dropping legacy index",t)
                  t
                }
              ))
              .getOrElse(Future.successful(()))
      count <- j.count(Option(q))
      wr <- traverse(count)
    } yield wr

    eventuallyUpgrade.onComplete {
      case Success(wrs) if wrs.exists(w => w.inError || w.hasErrors) =>
        val errors = wrs.filter(_.inError).map(r => s"${r.code} - ${r.message}").mkString("\n")
        logger.error("Upgrade did not complete successfully")
        logger.error(s"Errors during journal auto-upgrade:\n$errors")
        val writeErrors = wrs.filter(_.hasErrors).flatMap(_.writeErrors).map(we => s"${we.code} - ${we.errmsg}").mkString("\n")
        logger.error(s"Received ${wrs.count(_.hasErrors)} write errors during journal auto-upgrade:\n$writeErrors")
      case Success(wrs) =>
        val successCount = wrs.foldLeft(0)((sum,wr) => sum + wr.n)
        logger.info(s"Successfully upgraded $successCount records")
      case Failure(t) =>
        logger.error(s"Upgrade did not complete successfully",t)
    }

    logger.debug("Waiting on upgrade to complete...")

    Await.result(eventuallyUpgrade, 2.minutes) // ouch

    ()
  }

  private[mongodb] def closeConnections(): Unit = driver.close()

  private[mongodb] def dbName: String = databaseName.getOrElse(parsedMongoUri.db.getOrElse(DEFAULT_DB_NAME))
  private[mongodb] def failoverStrategy: FailoverStrategy = {
    val rxMSettings = rxSettings
    FailoverStrategy(
      initialDelay = rxMSettings.InitialDelay,
      retries = rxMSettings.Retries,
      delayFactor = rxMSettings.GrowthFunction
    )
  }
  private[mongodb] def db = connection(name = dbName, failoverStrategy = failoverStrategy)(system.dispatcher)

  private[mongodb] override def collection(name: String) = db[BSONCollection](name)
  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety,journalWTimeout,journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety,snapsWTimeout,snapsFsync)
  private[mongodb] def metadataWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)

  private[mongodb] override def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, keys: (String,Int)*)(implicit ec: ExecutionContext) = { collection =>
    val ky = keys.toSeq.map{ case (f,o) => f -> (if (o > 0) IndexType.Ascending else IndexType.Descending)}
    collection.indexesManager.ensure(new Index(
      key = ky,
      background = true,
      unique = unique,
      sparse = sparse,
      name = Some(indexName)))
    collection
  }

  override private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext) = {
    val collection = db[BSONCollection](name)
    collection.stats().flatMap{ case stats if ! stats.capped =>
      collection.convertToCapped(realtimeCollectionSize, None)
    }.recoverWith{ case _ =>
      collection.createCapped(realtimeCollectionSize, None)
    }
    collection
  }
}

class RxMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension {

  val driverProvider: RxMongoDriverProvider = new RxMongoDriverProvider

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    lazy val driver = new RxMongoDriver(actorSystem, config, driverProvider)

    override lazy val journaler = new RxMongoJournaller(driver) with MongoPersistenceJournalMetrics with MongoPersistenceJournalFailFast {
      override def driverName = "rxmongo"
      override private[mongodb] val breaker = driver.breaker
    }

    override lazy val snapshotter = new RxMongoSnapshotter(driver) with MongoPersistenceSnapshotFailFast {
      override private[mongodb] val breaker = driver.breaker
    }
    override lazy val readJournal = new RxMongoReadJournaller(driver)
  }

}

object RxMongoDriverSettings {
  def apply(systemSettings: ActorSystem.Settings) = {
    val fullName = s"${getClass.getPackage.getName}.rxmongo"
    val systemConfig = systemSettings.config
    systemConfig.checkValid(ConfigFactory.defaultReference(), fullName)
    new RxMongoDriverSettings(systemConfig.getConfig(fullName))
  }
}

class RxMongoDriverSettings(val config: Config) {

  config.checkValid(config, "failover")

  private val failover = config.getConfig("failover")
  def InitialDelay = {
    val configured = failover.getDuration("initialDelay")
    FiniteDuration(configured.toMillis, TimeUnit.MILLISECONDS)
  }
  def Retries = failover.getInt("retries")
  def Growth = failover.getString("growth")
  def ConstantGrowth = Growth == "con"
  def LinearGrowth = Growth == "lin"
  def ExponentialGrowth = Growth == "exp"
  def Factor = failover.getDouble("factor")

  def GrowthFunction: Int => Double = Growth match {
    case "con" => (i:Int) => Factor
    case "lin" => (i:Int) => i.toDouble
    case "exp" => (i:Int) => math.pow(i.toDouble, Factor)
  }
}