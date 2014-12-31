package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.WriteConcern

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

object CasbahPersistenceDriver {
  import akka.contrib.persistence.mongodb.MongoPersistenceBase._
  
  implicit def convertListOfStringsToListOfServerAddresses(strings: List[String]): List[ServerAddress] =
    strings.map { url =>
    val Array(host, port) = url.split(":")
    new ServerAddress(host, port.toInt)
    }.toList

  def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean): WriteConcern = (writeSafety,wtimeout.toMillis.toInt,fsync) match {
    case (ErrorsIgnored,w,f) => WriteConcern(-1,wTimeout = w, fsync = f,continueInsertOnError = true)
    case (Unacknowledged,w,f) => WriteConcern(0,wTimeout = w, fsync = f)
    case (Acknowledged,w,f) => WriteConcern(1,wTimeout = w, fsync = f)
    case (Journaled,w,_) => WriteConcern(1,j=true,wTimeout = w)
    case (ReplicaAcknowledged,w,f) => WriteConcern.withRule("majority",w,fsync = f,j = !f)
  }
}

trait CasbahPersistenceDriver extends MongoPersistenceDriver with MongoPersistenceBase {
  import akka.contrib.persistence.mongodb.CasbahPersistenceDriver._
  
  // Collection type
  type C = MongoCollection

  private[mongodb] lazy val client =
    userPass.map {
      case (u,p) => authenticated(u,p)
    }.getOrElse(unauthenticated())

  private[mongodb] lazy val db = client(mongoDbName)

  private[this] def authenticated(user: String, password: String) =
    MongoClient(mongoUrl,MongoCredential.createMongoCRCredential(user,mongoDbName,password.toCharArray) :: Nil)

  private[this] def unauthenticated() = MongoClient(mongoUrl)


  
  private[mongodb] def collection(name: String) = db(name)
  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety,journalWTimeout,journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety,snapsWTimeout,snapsFsync)
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
