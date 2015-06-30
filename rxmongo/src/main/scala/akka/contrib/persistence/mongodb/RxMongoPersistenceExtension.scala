package akka.contrib.persistence.mongodb

import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.GetLastError.WaitForAknowledgments
import reactivemongo.api.commands.WriteConcern
import reactivemongo.bson._
import reactivemongo.bson.buffer.ArrayReadableBuffer

import akka.actor.ActorSystem
import reactivemongo.api.commands.GetLastError
import reactivemongo.core.nodeset.Authenticate

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

object RxMongoPersistenceExtension {
  implicit object BsonBinaryHandler extends BSONHandler[BSONBinary, Array[Byte]] {
    def read(bson: reactivemongo.bson.BSONBinary): Array[Byte] = {
      val buffer = bson.value
      buffer.readArray(buffer.size)
    }

    def write(t: Array[Byte]): reactivemongo.bson.BSONBinary =
      BSONBinary(ArrayReadableBuffer(t), Subtype.GenericBinarySubtype)
  }
}

object RxMongoPersistenceDriver {
  import MongoPersistenceBase._

  def toGetLastError(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean):WriteConcern = (writeSafety,wtimeout.toMillis.toInt,fsync) match {
    case (ErrorsIgnored,wt,f) =>
      GetLastError(j = false, w = WaitForAknowledgments(0), wtimeout = Some(wt), fsync = f)
    case (Unacknowledged,wt,f) =>
      GetLastError(j = false, w = WaitForAknowledgments(0), wtimeout = Some(wt), fsync = f)
    case (Acknowledged,wt,f) =>
      GetLastError(j = false, w = WaitForAknowledgments(1), wtimeout = Some(wt), fsync = f)
    case (Journaled,wt,_) =>
      GetLastError(j = true, w = WaitForAknowledgments(1), wtimeout = Some(wt), fsync = false)
    case (ReplicaAcknowledged,wt,f) =>
      GetLastError(j = !f, w = WaitForAknowledgments(2), wtimeout = Some(wt), fsync = f)
  }
}

trait RxMongoPersistenceDriver extends MongoPersistenceDriver with MongoPersistenceBase {
  import RxMongoPersistenceDriver._

  // Collection type
  type C = BSONCollection

  private[mongodb] lazy val driver = MongoDriver()
  private[mongodb] lazy val connection =
    userPass.map {
      case (u,p) => authenticated(u,p)
    }.getOrElse(unauthenticated())

  private[this] def authenticated(user: String, pass: String) =
    driver.connection(mongoUrl,authentications = Seq(Authenticate(mongoDbName,user,pass)))
  private[this] def unauthenticated() = driver.connection(mongoUrl)

  private[mongodb] lazy val db = connection(mongoDbName)(actorSystem.dispatcher)

  private[mongodb] override def collection(name: String) = db[BSONCollection](name)
  private[mongodb] def journalWriteConcern: WriteConcern = toGetLastError(journalWriteSafety,journalWTimeout,journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toGetLastError(snapsWriteSafety,snapsWTimeout,snapsFsync)

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
