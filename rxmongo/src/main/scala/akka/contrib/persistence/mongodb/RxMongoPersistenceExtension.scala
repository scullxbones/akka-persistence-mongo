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
import reactivemongo.api.commands.CommandError
import reactivemongo.api.indexes.{Index, IndexType}
import reactivemongo.bson._

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

  private[mongodb] lazy val connection: MongoConnection =
    // authenticate and wait for confirmation
    wait {
      driver.connection(parsedMongoUri, strictUri = false).get.database(name = dbName, failoverStrategy = failoverStrategy).map(_.connection)
    }

  private[this] def wait[T](awaitable: Awaitable[T])(implicit duration: Duration): T =
    Await.result(awaitable, duration)

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
  private[mongodb] def db(implicit ec: ExecutionContext): Future[DefaultDB] =
    connection.database(name = dbName, failoverStrategy = failoverStrategy)

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
    collection.flatMap(c => c.indexesManager.ensure(Index(
      key = ky,
      background = true,
      unique = unique,
      sparse = sparse,
      name = Some(indexName))).map(_ => c))
  }

  override private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext) =
    for {
      cc <- ensureCollection(name, _.createCapped(realtimeCollectionSize, None))
      s <- cc.stats
      _ <- if (s.capped) Future.successful(()) else cc.convertToCapped(realtimeCollectionSize, None)
    } yield cc

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
  
  private[mongodb] def getSnapshotCollections()(implicit ec: ExecutionContext) = getCollections(snapsCollectionName)
  
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
