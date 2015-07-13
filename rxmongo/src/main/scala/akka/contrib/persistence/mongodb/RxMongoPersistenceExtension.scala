package akka.contrib.persistence.mongodb

import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteConcern
import reactivemongo.api.indexes.{IndexType, Index}
import reactivemongo.bson._

import akka.actor.ActorSystem

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
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

trait RxMongoPersistenceDriver extends MongoPersistenceDriver {
  import RxMongoPersistenceDriver._
  import concurrent.Await
  import concurrent.duration._

  // Collection type
  type C = BSONCollection

  type D = BSONDocument

  private[mongodb] lazy val driver = MongoDriver()
  private[this] lazy val parsedMongoUri = MongoConnection.parseURI(mongoUri) match {
    case Success(parsed) => parsed
    case Failure(throwable) => throw throwable
  }
  private[mongodb] lazy val connection =
    waitForPrimary(driver.connection(parsedURI = parsedMongoUri))

  private[this] def waitForPrimary(conn: MongoConnection): MongoConnection = {
    Await.result(conn.waitForPrimary(3.seconds),4.seconds)
    conn
  }

  private[mongodb] def db = connection(parsedMongoUri.db.getOrElse(DEFAULT_DB_NAME))(actorSystem.dispatcher)

  private[mongodb] override def collection(name: String) = db[BSONCollection](name)
  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety,journalWTimeout,journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety,snapsWTimeout,snapsFsync)

  private[mongodb] override def ensureUniqueIndex(collection: C, indexName: String, keys: (String,Int)*)(implicit ec: ExecutionContext) = {
    val ky:Seq[(String,IndexType)] = keys.map { case(f,o) => f -> (if (o > 0) IndexType.Ascending else IndexType.Descending) }
    collection.indexesManager.ensure(new Index(
      key = ky,
      background = true,
      unique = true,
      name = Some(indexName)))
    collection
  }
}

class RxMongoDriver(val actorSystem: ActorSystem) extends RxMongoPersistenceDriver {
  actorSystem.registerOnTermination {
    driver.close()
  }
}

class RxMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension {

  private[this] lazy val driver = new RxMongoDriver(actorSystem)
  private[this] lazy val _journaler = new RxMongoJournaller(driver) with MongoPersistenceJournalMetrics with MongoPersistenceJournalFailFast {
    override def driverName = "rxmongo"
    override private[mongodb] val breaker = driver.breaker
  }
  private[this] lazy val _snapshotter = new RxMongoSnapshotter(driver) with MongoPersistenceSnapshotFailFast {
    override private[mongodb] val breaker = driver.breaker
  }

  override def journaler = _journaler
  override def snapshotter = _snapshotter
} 
