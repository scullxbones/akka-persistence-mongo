/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import reactivemongo.api._
import reactivemongo.api.bson.collection.{BSONCollection, BSONSerializationPack}
import reactivemongo.api.commands.{Command, CommandError, WriteConcern}
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.api.bson.{BSONDocument, _}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
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
  val driver: AsyncDriver = {
    val md = AsyncDriver()
    actorSystem.registerOnTermination(driver.close()(actorSystem.dispatcher))
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

  private[mongodb] lazy val connection: Future[MongoConnection] =
    driver.connect(parsedMongoUri)

  private[mongodb] def closeConnections(): Unit = {
    driver.close(5.seconds)
    ()
  }

  private[mongodb] def dbName: String = databaseName.getOrElse(parsedMongoUri.db.getOrElse(DEFAULT_DB_NAME))
  private[mongodb] def failoverStrategy: FailoverStrategy = {
    val rxMSettings = rxSettings
    FailoverStrategy(
      initialDelay = rxMSettings.InitialDelay,
      retries = rxMSettings.Retries,
      delayFactor = rxMSettings.GrowthFunction)
  }
  private[mongodb] def db(implicit ec: ExecutionContext): Future[DefaultDB] =
    for {
      conn <- connection
      db   <- conn.database(name = dbName, failoverStrategy = failoverStrategy)
    } yield db

  private[mongodb] override def collection(name: String)(implicit ec: ExecutionContext) = db.map(_[BSONCollection](name))

  private[mongodb] override def ensureCollection(name: String)(implicit ec: ExecutionContext): Future[BSONCollection] =
    ensureCollection(name, _.create())

  private[this] def ensureCollection(name: String, collectionCreator: BSONCollection => Future[Unit])
                                    (implicit ec: ExecutionContext): Future[BSONCollection] = {
    for {
      coll <- collection(name)
      _ <- collectionCreator(coll).recover { case CommandError.Code(MongoErrors.NamespaceExists.code) => coll }
    } yield coll
  }

  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety, snapsWTimeout, snapsFsync)
  private[mongodb] def metadataWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)

  private[mongodb] override def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, keys: (String, Int)*)(implicit ec: ExecutionContext) = { collection =>
    val ky = keys.toSeq.map { case (f, o) => f -> (if (o > 0) IndexType.Ascending else IndexType.Descending) }
    collection.flatMap(c => c.indexesManager.ensure(Index(BSONSerializationPack)(
      key = ky,
      background = true,
      unique = unique,
      sparse = sparse,
      name = Some(indexName),
      dropDups = true,
      version = None,
      partialFilter = None,
      options = BSONDocument.empty
    )).map(_ => c))
  }

  override private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext) =
    for {
      cc <- ensureCollection(name, _.createCapped(realtimeCollectionSize, None))
      s <- cc.stats
      _ <- if (s.capped) Future.successful(()) else cc.convertToCapped(realtimeCollectionSize, None)
    } yield cc

  private[mongodb] def getCollectionsAsFuture(collectionName: String)(implicit ec: ExecutionContext): Future[List[BSONCollection]] = {
    getAllCollectionsAsFuture(Option(_.startsWith(collectionName)))
  }

  private[mongodb] def getJournalCollections()(implicit ec: ExecutionContext) =
    getCollectionsAsFuture(journalCollectionName)

  private[mongodb] def getAllCollectionsAsFuture(nameFilter: Option[String => Boolean])(implicit ec: ExecutionContext): Future[List[BSONCollection]] = {
    def excluded(name: String): Boolean =
      name == realtimeCollectionName ||
        name == metadataCollectionName ||
        name.startsWith("system.")

    def allPass(name: String): Boolean = true

    for {
      database  <- db
      names     <- database.collectionNames
      list      <- Future.sequence(names.filterNot(excluded).filter(nameFilter.getOrElse(allPass)).map(collection))
    } yield list
  }

  private[mongodb] def journalCollectionsAsFuture(implicit ec: ExecutionContext) = getCollectionsAsFuture(journalCollectionName)
  
  private[mongodb] def getSnapshotCollections()(implicit ec: ExecutionContext) = getCollectionsAsFuture(snapsCollectionName)

  private[mongodb] def removeEmptyJournal(jnl: BSONCollection)(implicit ec: ExecutionContext): Future[Unit] =
    removeEmptyCollection(jnl, journalIndexName)

  private[mongodb] def removeEmptySnapshot(snp: BSONCollection)(implicit ec: ExecutionContext): Future[Unit] =
    removeEmptyCollection(snp, snapsIndexName)

  private[this] var mongoVersion: Option[String] = None
  private[this] def getMongoVersion(implicit ec: ExecutionContext): Future[String] = mongoVersion match {
    case Some(v) => Future.successful(v)
    case None =>
      db.flatMap { database =>
        val runner = Command.run(BSONSerializationPack, FailoverStrategy())
        runner.apply(database, runner.rawCommand(BSONDocument("buildInfo" -> 1)))
          .one[BSONDocument](ReadPreference.Primary)
          .map(_.getAsOpt[BSONString]("version").getOrElse(BSONString("")).value)
          .map { v =>
            mongoVersion = Some(v)
            v
          }
      }
  }

  private[this] def isMongoVersionAtLeast(inputNbs: Int*)(implicit ec: ExecutionContext): Future[Boolean] =
    getMongoVersion.map {
      case str if str.isEmpty => false
      case str =>
        val versionNbs = str.split('.').map(_.toInt)
        inputNbs.zip(versionNbs).forall { case (i,v) => v >= i }
    }

  private[this] def removeEmptyCollection(collection: BSONCollection, indexName: String)(implicit ec: ExecutionContext): Future[Unit] =
    for {
      // first count, may be inaccurate in cluster environment
      firstCount <- collection.count(None, None, 0, None, ReadConcern.Local)
      // just to be sure: second count, always accurate and should be fast as we are pretty sure the result is zero
      secondCount <- if (firstCount == 0L) {
          for {
            b36 <- isMongoVersionAtLeast(3,6)
            count <- if (b36) {
                        collection.count(None, None, 0, Some(collection.hint(indexName)), ReadConcern.Majority)
                      } else {
                        collection.count(None, None, 0, None, ReadConcern.Majority)
                      }
          } yield count
        } else Future.successful(firstCount)
      if secondCount == 0L
      _ <- collection.drop(failIfNotFound = false)
    } yield ()
  
}

class RxMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension {

  val driverProvider: RxMongoDriverProvider = new RxMongoDriverProvider(actorSystem)

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    lazy val driver = new RxMongoDriver(actorSystem, config, driverProvider)

    override lazy val journaler: MongoPersistenceJournallingApi = new RxMongoJournaller(driver) with MongoPersistenceJournalMetrics {
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
