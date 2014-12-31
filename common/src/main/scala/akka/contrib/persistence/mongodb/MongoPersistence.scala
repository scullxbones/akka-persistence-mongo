package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.pattern.CircuitBreaker
import akka.serialization.SerializationExtension
import com.codahale.metrics.SharedMetricRegistries
import com.typesafe.config.ConfigFactory

import scala.language.implicitConversions

object MongoPersistenceBase {

  sealed trait WriteSafety
  case object ErrorsIgnored extends WriteSafety
  case object Unacknowledged extends WriteSafety
  case object Acknowledged extends WriteSafety
  case object Journaled extends WriteSafety
  case object ReplicaAcknowledged extends WriteSafety
  
  implicit def string2WriteSafety(fromConfig: String): WriteSafety = fromConfig.toLowerCase match {
    case "errorsignored" => ErrorsIgnored
    case "unacknowledged" => Unacknowledged
    case "acknowledged" => Acknowledged
    case "journaled" => Journaled
    case "replicaacknowledged" => ReplicaAcknowledged
  }
  
  private[mongodb] lazy val registry = SharedMetricRegistries.getOrCreate("mongodb")
}


trait MongoPersistenceDriver {
  // Collection type
  type C
  
  private[mongodb] def collection(name: String): C
}

trait MongoPersistenceBase {
  import akka.contrib.persistence.mongodb.MongoPersistenceBase._
  
  val actorSystem: ActorSystem

  val userOverrides = ConfigFactory.load()
  
  lazy val settings = new MongoSettings(actorSystem.settings, userOverrides)
  
  def snapsCollectionName = settings.SnapsCollection
  def snapsIndexName = settings.SnapsIndex
  def snapsWriteSafety: WriteSafety = settings.SnapsWriteConcern
  def snapsWTimeout = settings.SnapsWTimeout
  def snapsFsync = settings.SnapsFSync
  def journalCollectionName = settings.JournalCollection
  def journalIndexName = settings.JournalIndex
  def journalWriteSafety: WriteSafety = settings.JournalWriteConcern
  def journalWTimeout = settings.JournalWTimeout
  def journalFsync = settings.JournalFSync
  def mongoUrl = settings.Urls
  def mongoDbName = settings.DbName
  def userPass: Option[(String,String)] = {
    val userAndPass = (settings.Username,settings.Password)
    for {
      user <- userAndPass._1
      pass <- userAndPass._2
    } yield(user,pass)
  }
  
  lazy val serialization = SerializationExtension.get(actorSystem)
  lazy val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)
}