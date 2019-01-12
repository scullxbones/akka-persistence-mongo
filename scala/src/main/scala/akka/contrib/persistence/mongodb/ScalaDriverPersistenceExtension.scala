package akka.contrib.persistence.mongodb

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.MongoPersistenceDriver.{Acknowledged, Journaled, ReplicaAcknowledged, Unacknowledged, WriteSafety}
import akka.stream.ActorMaterializer
import com.mongodb.ConnectionString
import com.mongodb.client.model.{CreateCollectionOptions, IndexOptions}
import com.typesafe.config.Config
import org.mongodb.scala.bson.{BsonBoolean, BsonDocument}
import org.mongodb.scala.model.Indexes._
import org.mongodb.scala.{MongoClientSettings, _}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class ScalaMongoDriver(system: ActorSystem, config: Config) extends MongoPersistenceDriver(system, config) {
  override type C = Future[MongoCollection[D]]
  override type D = BsonDocument

  val ScalaSerializers: ScalaDriverSerializers = ScalaDriverSerializersExtension(system)
  val scalaDriverSettings: ScalaDriverSettings = ScalaDriverSettings(system)

  private def mongoClientSettings: MongoClientSettings =
    scalaDriverSettings
      .configure(MongoClientSettings.builder())
      .applyConnectionString(new ConnectionString(mongoUri))
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

  override private[mongodb] def collection(name: String): C =
    Future.successful(db.getCollection(name))

  override private[mongodb] def ensureCollection(name: String): C = {
    implicit val ec: ExecutionContext = system.dispatcher
    db.listCollectionNames().toFuture().flatMap {
      case xs if xs.contains(name) =>
        collection(name)
      case _ =>
        db.createCollection(name).toFuture().flatMap(_ => collection(name))
    }
  }

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
    def createCappedCollection(): C = {
      val options = new CreateCollectionOptions().capped(true).sizeInBytes(realtimeCollectionSize)
      db.createCollection(name, options)
        .toFuture()
        .flatMap(_ => collection(name))
    }

    db.listCollections().filter(BsonDocument("name" -> name)).toFuture().flatMap { collections =>
      val capped = collections.headOption
        .flatMap(d => d.get("options"))
        .collect{ case d: BsonDocument if d.containsKey("capped") => d.get("capped").asBoolean() }
      if (capped.contains(BsonBoolean(true))) {
        collection(name)
      } else if (capped.isDefined) {
        collection(name)
          .flatMap(_.drop().toFuture())
          .flatMap(_ => createCappedCollection())
      } else {
        createCappedCollection()
      }
    }
  }

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
      list  =   names.filterNot(excluded).filter(nameFilter.getOrElse(passAll _))
      xs    <-  Future.sequence(list.map(collection))
    } yield xs.toList
  }

  private[mongodb] def journalCollectionsAsFuture(implicit ec: ExecutionContext) = getCollectionsAsFuture(journalCollectionName)

  private[mongodb] def snapshotCollectionsAsFuture(implicit ec: ExecutionContext) = getCollectionsAsFuture(snapsCollectionName)

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
