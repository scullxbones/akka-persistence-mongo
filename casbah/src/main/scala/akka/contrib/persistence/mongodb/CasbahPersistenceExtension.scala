package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.WriteConcern

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object CasbahPersistenceDriver {
  import MongoPersistenceDriver._
  
  def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean): WriteConcern = (writeSafety,wtimeout.toMillis.toInt,fsync) match {
    case (Unacknowledged,w,f) => new WriteConcern(0, w, f)
    case (Acknowledged,w,f) => new WriteConcern(1, w, f)
    case (Journaled,w,_) => new WriteConcern(1,w,false,true)
    case (ReplicaAcknowledged,w,f) => WriteConcern.majorityWriteConcern(w,f,!f)
  }
}

trait CasbahPersistenceDriver extends MongoPersistenceDriver {
  import akka.contrib.persistence.mongodb.CasbahPersistenceDriver._
  
  // Collection type
  type C = MongoCollection

  type D = DBObject

  private[this] lazy val url = MongoClientURI(mongoUri)

  private[mongodb] lazy val client = MongoClient(url)

  private[mongodb] lazy val db = client(url.database.getOrElse(DEFAULT_DB_NAME))


  
  private[mongodb] def collection(name: String) = db(name)
  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety,journalWTimeout,journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety,snapsWTimeout,snapsFsync)


  private[mongodb] override def ensureUniqueIndex(collection: C, indexName: String, keys: (String,Int)*)(implicit ec: ExecutionContext): MongoCollection = {
    collection.createIndex(
      MongoDBObject(keys :_*),
      MongoDBObject("unique" -> true, "name" -> indexName))
    collection
  }

}

class CasbahMongoDriver(val actorSystem: ActorSystem) extends CasbahPersistenceDriver {
  actorSystem.registerOnTermination {
    client.close()
  }
}

class CasbahPersistenceExtension(val actorSystem: ActorSystem) extends MongoPersistenceExtension {
  private[this] lazy val driver = new CasbahMongoDriver(actorSystem)
  private[this] lazy val _journaler =
    new CasbahPersistenceJournaller(driver) with MongoPersistenceJournalMetrics with MongoPersistenceJournalFailFast {
      override def driverName = "casbah"
      override private[mongodb] val breaker = driver.breaker
    }
  private[this] lazy val _snapshotter = new CasbahPersistenceSnapshotter(driver) with MongoPersistenceSnapshotFailFast {
    override private[mongodb] val breaker = driver.breaker
  }
  
  override def journaler = _journaler
  override def snapshotter = _snapshotter
}
