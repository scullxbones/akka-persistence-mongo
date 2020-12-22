package akka.contrib.persistence.mongodb

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.MongoPersistenceDriver.{Acknowledged, Journaled, ReplicaAcknowledged, Unacknowledged, WriteSafety}
import com.mongodb.ConnectionString
import com.mongodb.client.model.{CreateCollectionOptions, IndexOptions}
import com.typesafe.config.Config
import org.mongodb.scala.bson.{BsonDocument, BsonString}
import org.mongodb.scala.model.CountOptions
import org.mongodb.scala.model.Indexes._
import org.mongodb.scala.{MongoClientSettings, _}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class ScalaMongoDriver(system: ActorSystem, config: Config) extends MongoPersistenceDriver(system, config) {
  override type C = MongoCollection[BsonDocument]
  override type D = BsonDocument

  val ScalaSerializers: ScalaDriverSerializers = ScalaDriverSerializersExtension(system)
  val scalaDriverSettings: ScalaDriverSettings = ScalaDriverSettings(system)

  val mongoClientSettings: MongoClientSettings =
    scalaDriverSettings
      .configure(mongoUri)
      .build()

  lazy val client: MongoClient = MongoClient(mongoClientSettings)
  lazy val db: MongoDatabase = {
    val dbName =
      databaseName.orElse(
        Option(new ConnectionString(mongoUri).getDatabase)
      ).getOrElse(DEFAULT_DB_NAME)
    client.getDatabase(dbName)
  }

  override def collection(name: String): Future[C] =
    Future.successful(db.getCollection(name))

  override def ensureCollection(name: String): Future[C] =
    ensureCollection(name, db.createCollection)

  private[this] def ensureCollection(name: String, collectionCreator: String => SingleObservable[Void]): Future[C] =
    for {
      _ <- collectionCreator(name).toFuture().recover { case MongoErrors.NamespaceExists() => () }
      mongoCollection <- collection(name)
    } yield mongoCollection

  def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety, snapsWTimeout, snapsFsync)
  def metadataWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  private def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean): WriteConcern =
    (writeSafety, wtimeout.toMillis, fsync) match {
      case (Unacknowledged, w, f)      => WriteConcern.UNACKNOWLEDGED.withWTimeout(w, TimeUnit.MILLISECONDS)
      case (Acknowledged, w, f)        => WriteConcern.ACKNOWLEDGED.withWTimeout(w, TimeUnit.MILLISECONDS)
      case (Journaled, w, _)           => WriteConcern.JOURNALED.withWTimeout(w, TimeUnit.MILLISECONDS)
      case (ReplicaAcknowledged, w, f) => WriteConcern.MAJORITY.withWTimeout(w, TimeUnit.MILLISECONDS).withJournal(!f)
    }

  override def cappedCollection(name: String): Future[C] = {
    val cappedCollectionCreator = (ccName: String) =>
      db.createCollection(ccName, new CreateCollectionOptions().capped(true).sizeInBytes(realtimeCollectionSize))

    def recreate(collection: C): Future[C] =
      for {
        _ <- collection.drop().toFuture()
        recreatedCappedCollection <- ensureCollection(name, cappedCollectionCreator)
      } yield recreatedCappedCollection

    for {
      collection  <- ensureCollection(name, cappedCollectionCreator)
      capped      <- isCappedCollection(name)
      cc          <- if (capped) Future.successful(collection) else recreate(collection)
    } yield cc
  }

  private[this] def isCappedCollection(collectionName: String): Future[Boolean] =
    db.runCommand(BsonDocument("collStats" -> collectionName))
      .toFuture()
      .map(stats => stats.get("capped").exists(_.asBoolean.getValue))

  override def collectionNames: Future[List[String]] =
    db.listCollectionNames().toFuture().map(_.toList)

  override def getMongoVersionFromBuildInfo: Future[String] =
    db.runCommand(BsonDocument("buildInfo" -> 1)).toFuture()
      .map(_.get("version").getOrElse(BsonString("")).asString().getValue)

  private[this] def getLocalCount(collection: MongoCollection[D]): Future[Long] = {
    db.runCommand(BsonDocument("count" -> s"${collection.namespace.getCollectionName}", "readConcern" -> BsonDocument("level" -> "local")))
      .toFuture()
      .map(_.getOrElse("n", 0L).asInt32().longValue())
  }

  private[this] def getIndexAsBson(collection: MongoCollection[D], indexName: String): Future[Option[BsonDocument]] =
    for {
      indexList <- collection.listIndexes[BsonDocument]().toFuture()
      indexDoc = indexList.find(_.get("name").asString().getValue.equals(indexName))
      indexKey = indexDoc match {
          case Some(doc) => Some(doc.get("key").asDocument())
          case None => None
        }
    } yield indexKey

  override def removeEmptyCollection(collection: MongoCollection[D], indexName: String): Future[Unit] =
    for {
      version <- mongoVersion
      // first count, may be inaccurate in cluster environment
      firstCount <- if (version.atLeast(ServerVersion.`4.0`("3"))) {
          collection.estimatedDocumentCount().toFuture()
        } else {
          getLocalCount(collection)
        }
      // just to be sure: second count, always accurate and should be fast as we are pretty sure the result is zero
      secondCount <- if (firstCount == 0L) {
          for {
            indexKey <- getIndexAsBson(collection, indexName)
            count <- if (version.atLeast(ServerVersion.`3.6.0`)) {
                indexKey.fold(collection.countDocuments())(index =>
                  collection.countDocuments(BsonDocument(), CountOptions().hint(index))
                ).toFuture()
              } else collection.countDocuments().toFuture()
          } yield count
        } else Future.successful(firstCount)
        _ = if (secondCount == 0L) collection.drop().toFuture().recover { case _ => () } // ignore errors
    } yield ()

  override def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, fields: (String, Int)*): C => Future[C] = {
    collection =>
      for {
        _ <- collection.createIndex(
          compoundIndex(fields.map {
            case (name, d) if d > 0 => ascending(name)
            case (name, _) => descending(name)
          }: _*),
          new IndexOptions().unique(unique).sparse(sparse).name(indexName)
        ).toFuture()
      } yield collection
  }

  override def closeConnections(): Unit =
    client.close()
}

class ScalaDriverPersistenceExtension(val actorSystem: ActorSystem) extends MongoPersistenceExtension(actorSystem) {

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    val driver = new ScalaMongoDriver(actorSystem, config)

    override lazy val journaler: MongoPersistenceJournallingApi = new ScalaDriverPersistenceJournaller(driver) with MongoPersistenceJournalMetrics {
      override def driverName = "scala-official"
    }
    override lazy val snapshotter = new ScalaDriverPersistenceSnapshotter(driver)

    override lazy val readJournal = new ScalaDriverPersistenceReadJournaller(driver)
  }

}
