/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.{BasicDBObjectBuilder, DBCollection, WriteConcern, MongoClientURI => JavaMongoClientURI}
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

object CasbahPersistenceDriver {
  import MongoPersistenceDriver._

  def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean): WriteConcern = (writeSafety, wtimeout.toMillis.toInt, fsync) match {
    case (Unacknowledged, w, f)      => new WriteConcern(0, w, f)
    case (Acknowledged, w, f)        => new WriteConcern(1, w, f)
    case (Journaled, w, _)           => new WriteConcern(1, w, false, true)
    case (ReplicaAcknowledged, w, f) => WriteConcern.majorityWriteConcern(w, f, !f)
  }
}

class CasbahMongoDriver(system: ActorSystem, config: Config) extends MongoPersistenceDriver(system, config) {
  import akka.contrib.persistence.mongodb.CasbahPersistenceDriver._

  val CasbahSerializers: CasbahSerializers = CasbahSerializersExtension(system)

  // Collection type
  type C = MongoCollection

  type D = DBObject

  override private[mongodb] def closeConnections(): Unit = client.close()

  private[this] val casbahSettings = CasbahDriverSettings(system)

  private[this] val url = {
    val underlying =  new JavaMongoClientURI(mongoUri,casbahSettings.configure(new MongoClientOptions.Builder()))
    MongoClientURI(underlying)
  }

  private[mongodb] lazy val client = MongoClient(url)

  private[mongodb] lazy val db = client(databaseName.getOrElse(url.database.getOrElse(DEFAULT_DB_NAME)))

  private[mongodb] override def collection(name: String)(implicit ec: ExecutionContext) = db(name)

  private[mongodb] override def ensureCollection(name: String)(implicit ec: ExecutionContext): MongoCollection =
    ensureCollection(name, collectionName => db.createCollection(collectionName, BasicDBObjectBuilder.start().get()))

  private[this] def ensureCollection(name: String, collectionCreator: String => DBCollection)
                                    (implicit ec: ExecutionContext): MongoCollection =
    Try(collectionCreator(name).asScala) match {
      case Success(collection) => collection
      case Failure(MongoErrors.NamespaceExists()) => db(name)
      case Failure(error) => throw error
    }

  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety, snapsWTimeout, snapsFsync)
  private[mongodb] def metadataWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety, journalWTimeout, journalFsync)

  private[mongodb] override def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, fields: (String, Int)*)(implicit ec: ExecutionContext): C => C = { collection =>
    collection.createIndex(
      Map(fields: _*),
      Map("unique" -> unique, "sparse" -> sparse, "name" -> indexName))
    collection
  }

  override private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext) = {
    val collection = ensureCollection(name, createNewCappedCollection)
    if (collection.isCapped) {
      collection
    } else {
      collection.drop()
      ensureCollection(name, createNewCappedCollection)
    }
  }

  private[this] def createNewCappedCollection(name: String): DBCollection = {
    val collection = db.createCollection(name,
      BasicDBObjectBuilder.start.add("capped", true).add("size", realtimeCollectionSize).get())
    collection.insert(MongoDBObject("x" -> "x")) // casbah cannot tail empty collections
    collection
  }

  private[mongodb] def getCollections(collectionName: String)(implicit ec: ExecutionContext): List[C] = {
    def excludeNames(name: String): Boolean =
      name == realtimeCollectionName ||
        name == metadataCollectionName ||
        name.startsWith("system.")

    db.collectionNames().filterNot(excludeNames).filter(_.startsWith(collectionName)).map(collection).toList
  }

  private[mongodb] def getJournalCollections(implicit ec: ExecutionContext): List[C] = getCollections(journalCollectionName)

  private[mongodb] def getSnapshotCollections(implicit ec: ExecutionContext): List[C] = getCollections(snapsCollectionName)

}

class CasbahPersistenceExtension(val actorSystem: ActorSystem) extends MongoPersistenceExtension {

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    val driver = new CasbahMongoDriver(actorSystem, config)

    override lazy val journaler: MongoPersistenceJournallingApi = new CasbahPersistenceJournaller(driver) with MongoPersistenceJournalMetrics {
      override def driverName = "casbah"
    }
    override lazy val snapshotter = new CasbahPersistenceSnapshotter(driver)

    override lazy val readJournal = new CasbahPersistenceReadJournaller(driver)
  }

}
