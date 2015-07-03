package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.pattern.CircuitBreaker
import akka.serialization.SerializationExtension
import com.codahale.metrics.SharedMetricRegistries

import scala.language.implicitConversions

object MongoPersistenceBase {

  sealed trait WriteSafety
  case object Unacknowledged extends WriteSafety
  case object Acknowledged extends WriteSafety
  case object Journaled extends WriteSafety
  case object ReplicaAcknowledged extends WriteSafety
  
  implicit def string2WriteSafety(fromConfig: String): WriteSafety = fromConfig.toLowerCase match {
    case "errorsignored" => throw new IllegalArgumentException("Errors ignored is no longer supported as a write safety option")
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

  lazy val settings = new MongoSettings(actorSystem.settings)
  
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
  def mongoUri = settings.MongoUri

  val DEFAULT_DB_NAME = "akka-persistence"

  lazy val serialization = SerializationExtension.get(actorSystem)
  lazy val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)
}