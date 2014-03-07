package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import scala.language.implicitConversions

object MongoPersistenceBase {

  sealed trait WriteSafety
  case object ErrorsIgnored extends WriteSafety
  case object Unacknowledged extends WriteSafety
  case object Acknowledged extends WriteSafety
  case object Journaled extends WriteSafety
  case object ReplicaAcknowledged extends WriteSafety
  
  implicit def string2WriteSafety(fromConfig: String): WriteSafety = fromConfig.toLowerCase() match {
    case "errorsignored" => ErrorsIgnored
    case "unacknowledged" => Unacknowledged
    case "acknowledged" => Acknowledged
    case "journaled" => Journaled
    case "replicaacknowledged" => ReplicaAcknowledged
  }
}


trait MongoPersistenceDriver {
  // Collection type
  type C
  
  private[mongodb] def collection(name: String): C
}

trait MongoPersistenceBase {
  import MongoPersistenceBase._
  
  val actorSystem: ActorSystem
  
  private[this] lazy val settings = new MongoSettings(actorSystem.settings, ConfigFactory.load())
  
  def snapsCollectionName = settings.SnapsCollection
  def snapsIndexName = settings.SnapsIndex
  def snapsWriteSafety: WriteSafety = settings.SnapsWriteConcern
  def journalCollectionName = settings.JournalCollection
  def journalIndexName = settings.JournalIndex
  def journalWriteSafety: WriteSafety = settings.JournalWriteConcern 
  def mongoUrl = settings.Urls
  def mongoDbName = settings.DbName
  
  lazy val serialization = SerializationExtension.get(actorSystem)
  lazy val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)
}