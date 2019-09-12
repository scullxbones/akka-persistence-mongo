package akka.contrib.persistence.mongodb

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.MongoPersistenceDriver.{Acknowledged, Journaled, ReplicaAcknowledged, Unacknowledged, WriteSafety}
import akka.stream.ActorMaterializer
import com.mongodb.ConnectionString
import com.mongodb.client.model.{CreateCollectionOptions, IndexOptions}
import com.typesafe.config.Config
import org.mongodb.scala.bson.{BsonDocument, BsonString}
import org.mongodb.scala.model.CountOptions
import org.mongodb.scala.model.Indexes._
import org.mongodb.scala.{MongoClientSettings, _}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class ScalaMongoDriver(system: ActorSystem, config: Config) extends MongoPersistenceDriver(system, config) {
  override type C = Future[MongoCollection[D]]
  override type D = BsonDocument

  val ScalaSerializers: ScalaDriverSerializers = ScalaDriverSerializersExtension(system)
  val scalaDriverSettings: ScalaDriverSettings = ScalaDriverSettings(system)

  private[this] val mongoClientSettings: MongoClientSettings =
    scalaDriverSettings
      .configure(mongoUri)
      .applicationName("akka-persistence-mongodb")
      .build()

  private[mongodb] lazy val client = MongoClient(mongoClientSettings)
  private[mongodb] lazy val db: MongoDatabase = {
    val dbName =
      databaseName.orElse(
        Option(new ConnectionString(mongoUri).getDatabase)
      ).getOrElse(DEFAULT_DB_NAME)
    client.getDatabase(dbName)
  }

  override private[mongodb] def collection(name: String)(implicit ec: ExecutionContext): C =
    Future.successful(db.getCollection(name))

  override private[mongodb] def ensureCollection(name: String)(implicit ec: ExecutionContext): C =
    ensureCollection(name, db.createCollection)

  private[this] def ensureCollection(name: String, collectionCreator: String => SingleObservable[Completed])
                                    (implicit ec: ExecutionContext): C =
    for {
      _ <- collectionCreator(name).toFuture().recover { case MongoErrors.NamespaceExists() => Completed }
      mongoCollection <- collection(name)
    } yield mongoCollection

  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety, snapsWTimeout, snapsFsync)
  private[mongodb] def metadataWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  private def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean): WriteConcern =
    (writeSafety, wtimeout.toMillis, fsync) match {
      case (Unacknowledged, w, f)      => WriteConcern.UNACKNOWLEDGED.withWTimeout(w, TimeUnit.MILLISECONDS).withFsync(f)
      case (Acknowledged, w, f)        => WriteConcern.ACKNOWLEDGED.withWTimeout(w, TimeUnit.MILLISECONDS).withFsync(f)
      case (Journaled, w, _)           => WriteConcern.JOURNALED.withWTimeout(w, TimeUnit.MILLISECONDS)
      case (ReplicaAcknowledged, w, f) => WriteConcern.MAJORITY.withWTimeout(w, TimeUnit.MILLISECONDS).withJournal(!f)
    }

  override private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext): C = {
    val cappedCollectionCreator = (ccName: String) =>
      db.createCollection(ccName, new CreateCollectionOptions().capped(true).sizeInBytes(realtimeCollectionSize))
    for {
      collection <- ensureCollection(name, cappedCollectionCreator)
      capped <- isCappedCollection(name)
      cc <- if (capped) Future.successful(collection) else for {
        _ <- collection.drop().toFuture()
        recreatedCappedCollection <- ensureCollection(name, cappedCollectionCreator)
      } yield recreatedCappedCollection
    } yield cc
  }

  private[this] def isCappedCollection(collectionName: String): Future[Boolean] =
    db.runCommand(BsonDocument("collStats" -> collectionName))
      .toFuture()
      .map(stats => stats.get("capped").exists(_.asBoolean.getValue))

  private[mongodb] def getCollectionsAsFuture(collectionName: String)(implicit ec: ExecutionContext): Future[List[MongoCollection[D]]] = {
    getAllCollectionsAsFuture(Option(_.startsWith(collectionName)))
  }

  private[mongodb] def getAllCollectionsAsFuture(nameFilter: Option[String => Boolean])(implicit ec: ExecutionContext): Future[List[MongoCollection[D]]] = {
    def excluded(name: String): Boolean =
      name == realtimeCollectionName ||
        name == metadataCollectionName ||
        name.startsWith("system.")

    def passAll(name: String): Boolean = true

    for {
      names <-  db.listCollectionNames().toFuture()
      list  =   names.filterNot(excluded).filter(nameFilter.getOrElse(passAll))
      xs    <-  Future.sequence(list.map(collection))
    } yield xs.toList
  }

  private[mongodb] def journalCollectionsAsFuture(implicit ec: ExecutionContext) = getCollectionsAsFuture(journalCollectionName)

  private[mongodb] def snapshotCollectionsAsFuture(implicit ec: ExecutionContext) = getCollectionsAsFuture(snapsCollectionName)

  private[mongodb] def removeEmptyJournal(jnl: MongoCollection[D])(implicit ec: ExecutionContext): Future[Unit] =
    removeEmptyCollection(jnl, journalIndexName)

  private[mongodb] def removeEmptySnapshot(snp: MongoCollection[D])(implicit ec: ExecutionContext): Future[Unit] =
    removeEmptyCollection(snp, snapsIndexName)

  private[this] var mongoVersion: Option[String] = None
  private[this] def getMongoVersion: Future[String] = mongoVersion match {
    case Some(v) => Future.successful(v)
    case None =>
      db.runCommand(BsonDocument("buildInfo" -> 1)).toFuture()
        .map(_.get("version").getOrElse(BsonString("")).asString().getValue)
        .map { v =>
          mongoVersion = Some(v)
          v
        }
  }

  private[this] def isMongoVersionAtLeast(inputNbs: Int*)(implicit ec: ExecutionContext): Future[Boolean] =
    getMongoVersion.map {
        case str if str.isEmpty => false
        case str =>
          val versionNbs = str.split('.').map(_.toInt)
          inputNbs.zip(versionNbs).forall { case (i,v) => v >= i }
      }

  private[this] def getLocalCount(collection: MongoCollection[D])(implicit ec: ExecutionContext): Future[Long] = {
    db.runCommand(BsonDocument("count" -> s"${collection.namespace}", "readConcern" -> BsonDocument("level" -> "local")))
      .toFuture()
      .map(_.getOrElse("n", 0L).asInt32().longValue())
  }

  private[this] def getIndexAsBson(collection: MongoCollection[D], indexName: String)(implicit ec: ExecutionContext): Future[Option[BsonDocument]] =
    for {
      indexList <- collection.listIndexes[BsonDocument]().toFuture()
      indexDoc = indexList.find(_.get("name").asString().getValue.equals(indexName))
      indexKey = indexDoc match {
          case Some(doc) => Some(doc.get("key").asDocument())
          case None => None
        }
    } yield indexKey

  private[this] def removeEmptyCollection(collection: MongoCollection[D], indexName: String)(implicit ec: ExecutionContext): Future[Unit] =
    for {
      b403 <- isMongoVersionAtLeast(4,0,3)
      // first count, may be inaccurate in cluster environment
      firstCount <- if (b403) {
          collection.estimatedDocumentCount().toFuture()
        } else {
          getLocalCount(collection)
        }
      // just to be sure...
      if firstCount == 0L
        // second count, always accurate and should be fast as we are pretty sure the result is zero
        secondCount <-
          for {
            b36 <- isMongoVersionAtLeast(3,6)
            if b36 // lets optimize aggregate method, using appropriate index (that we have to grab from indexes list)
              indexKey <- getIndexAsBson(collection, indexName)
            count <- if (b36) {
                indexKey match {
                  case Some(index) => collection.countDocuments(BsonDocument(), CountOptions().hint(index)).toFuture()
                  case None => collection.countDocuments().toFuture()
                }
              } else {
                collection.countDocuments().toFuture()
              }
          } yield count
      if secondCount == 0L
        _ <- collection.drop().toFuture().recover { case _ => Completed() } // ignore errors
    } yield ()

  override private[mongodb] def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, fields: (String, Int)*)(implicit ec: ExecutionContext): C => C = { collection =>
    for {
      c <- collection
      _ <- c.createIndex(
        compoundIndex(fields.map {
          case (name, d) if d > 0 => ascending(name)
          case (name, _) => descending(name)
        }: _*),
        new IndexOptions().unique(unique).sparse(sparse).name(indexName)
      ).toFuture()
    } yield c
  }

  override private[mongodb] def closeConnections(): Unit =
    client.close()
}

class ScalaDriverPersistenceExtension(val actorSystem: ActorSystem) extends MongoPersistenceExtension {

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    val driver = new ScalaMongoDriver(actorSystem, config)

    override lazy val journaler: MongoPersistenceJournallingApi = new ScalaDriverPersistenceJournaller(driver) with MongoPersistenceJournalMetrics {
      override def driverName = "scala-official"
    }
    override lazy val snapshotter = new ScalaDriverPersistenceSnapshotter(driver)

    override lazy val readJournal = new ScalaDriverPersistenceReadJournaller(driver, ActorMaterializer()(actorSystem))
  }

}
