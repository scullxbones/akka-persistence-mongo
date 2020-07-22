/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import reactivemongo.api._
import reactivemongo.api.bson.collection.{BSONCollection, BSONSerializationPack}
import reactivemongo.api.bson.{BSONDocument, _}
import reactivemongo.api.indexes.{Index, IndexType}

import scala.concurrent.Future
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
  override type C = BSONCollection

  override type D = BSONDocument

  private def rxSettings = RxMongoDriverSettings(system.settings)
  private[mongodb] val driver = driverProvider.driver

  private[this] lazy val parsedMongoUri = MongoConnection.fromString(mongoUri)

  implicit val waitFor: FiniteDuration = 10.seconds

  lazy val connection: Future[MongoConnection] =
    parsedMongoUri.flatMap(driver.connect(_: MongoConnection.ParsedURI))

  def closeConnections(): Unit = {
    driver.close(5.seconds)
    ()
  }

  def failoverStrategy: FailoverStrategy = {
    val rxMSettings = rxSettings
    FailoverStrategy(
      initialDelay = rxMSettings.InitialDelay,
      retries = rxMSettings.Retries,
      delayFactor = rxMSettings.GrowthFunction)
  }

  def dbName: Future[String] = databaseName match {
    case Some(name) => Future.successful(name)
    case _ => parsedMongoUri.map(_.db getOrElse DEFAULT_DB_NAME)
  }

  def db: Future[DB] =
    for {
      conn <- connection
      nme <- dbName
      db   <- conn.database(name = nme, failoverStrategy = failoverStrategy)
    } yield db

  override def collection(name: String): Future[BSONCollection] =
    db.map(_[BSONCollection](name))

  override def ensureCollection(name: String): Future[BSONCollection] =
    ensureCollection(name, _.create())

  private[this] def ensureCollection(name: String, collectionCreator: BSONCollection => Future[Unit]): Future[BSONCollection] =
    for {
      coll <- collection(name)
      _ <- collectionCreator(coll).recover {
        case commands.CommandException.Code(
          MongoErrors.NamespaceExists.code) => coll
      }
    } yield coll

  def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety, snapsWTimeout, snapsFsync)
  def metadataWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)

  override def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, keys: (String, Int)*): C => Future[C] = {
    collection =>
      val ky = keys.toSeq.map { case (f, o) => f -> (if (o > 0) IndexType.Ascending else IndexType.Descending) }
      collection.indexesManager.ensure(Index(
        key = ky,
        background = true,
        unique = unique,
        sparse = sparse,
        name = Some(indexName),
        version = None,
        partialFilter = None,
        options = BSONDocument.empty
      )).map(_ => collection)
  }

  def collectionNames: Future[List[String]] =
    for {
      database  <- db
      names     <- database.collectionNames
    } yield names

  override def cappedCollection(name: String): Future[C] =
    for {
      cc <- ensureCollection(name, _.createCapped(realtimeCollectionSize, None))
      s  <- cc.stats
      _  <- if (s.capped) Future.successful(()) else cc.convertToCapped(realtimeCollectionSize, None)
    } yield cc

  def getMongoVersionFromBuildInfo: Future[String] =
      db.flatMap { database =>
        database.runCommand(BSONDocument("buildInfo" -> 1), FailoverStrategy())
          .one[BSONDocument](ReadPreference.Primary)
          .map(_.getAsOpt[BSONString]("version").getOrElse(BSONString("")).value)
      }

  def removeEmptyCollection(collection: C, indexName: String): Future[Unit] = {
    for {
      // first count, may be inaccurate in cluster environment
      firstCount <- collection.count(None, None, 0, None, ReadConcern.Local)
      // just to be sure: second count, always accurate and should be fast as we are pretty sure the result is zero
      secondCount <- if (firstCount == 0L) {
          for {
            version <- mongoVersion
            count <- if (version.atLeast(ServerVersion.`3.6.0`)) {
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
}

class RxMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension(actorSystem) {

  val driverProvider: RxMongoDriverProvider = new RxMongoDriverProvider(actorSystem)

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    lazy val driver = new RxMongoDriver(actorSystem, config, driverProvider)

    override lazy val journaler: MongoPersistenceJournallingApi = new RxMongoJournaller(driver) with MongoPersistenceJournalMetrics {
      override def driverName = "rxmongo"
    }

    override lazy val snapshotter = new RxMongoSnapshotter(driver)
    override lazy val readJournal = new RxMongoReadJournaller(driver)
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
