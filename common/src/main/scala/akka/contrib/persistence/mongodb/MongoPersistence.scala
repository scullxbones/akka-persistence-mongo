package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.pattern.CircuitBreaker
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import com.codahale.metrics.SharedMetricRegistries

import scala.collection.immutable.Seq
import scala.language.implicitConversions

object MongoPersistenceDriver {

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

trait CanSerialize[D] {
  import collection.immutable.{Seq => ISeq}

  def serializeAtomic(payload: ISeq[PersistentRepr])(implicit serialization: Serialization, system: ActorSystem): D
  def serializeRepr(repr: PersistentRepr)(implicit serialization: Serialization, system: ActorSystem): D
}

trait CanDeserialize[D] {
  def deserializeRepr(implicit serialization: Serialization, system: ActorSystem): PartialFunction[D,PersistentRepr]
}

trait Formats[D] extends CanSerialize[D] with CanDeserialize[D]

trait MongoPersistenceDriver {
  import MongoPersistenceDriver._

  // Collection type
  type C

  // Document type
  type D

  private[mongodb] def collection(name: String): C

  implicit val actorSystem: ActorSystem

  val settings = new MongoSettings(actorSystem.settings)
  
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

  implicit val serialization = SerializationExtension.get(actorSystem)
  val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)

  def deserialize(dbo: D)(implicit ev: CanDeserialize[D]) =
    ev.deserializeRepr.lift.apply(dbo).getOrElse(throw new IllegalArgumentException(s"Unable to deserialize document $dbo"))

  def serialize(aw: AtomicWrite)(implicit ev: CanSerialize[D]) =
    ev.serializeAtomic(aw.payload)

  def serialize(repr: PersistentRepr)(implicit ev: CanSerialize[D]) =
    ev.serializeRepr(repr)
}