package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.JournallingFieldNames.PROCESSOR_ID
import akka.contrib.persistence.mongodb.JournallingFieldNames.SEQUENCE_NUMBER
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.contrib.persistence.mongodb.SnapshottingFieldNames._
import akka.pattern.CircuitBreaker
import akka.serialization.{Serialization, SerializationExtension}
import com.codahale.metrics.SharedMetricRegistries

import scala.concurrent.ExecutionContext
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

trait CanSerializeJournal[D] {
  def serializeAtom(atoms: TraversableOnce[Atom])(implicit serialization: Serialization, system: ActorSystem): D
}

trait CanDeserializeJournal[D] {
  def deserializeDocument(document: D)(implicit serialization: Serialization, system: ActorSystem): Event
}

trait JournalFormats[D] extends CanSerializeJournal[D] with CanDeserializeJournal[D]

trait MongoPersistenceDriver {
  import MongoPersistenceDriver._

  // Collection type
  type C

  // Document type
  type D

  private[mongodb] def collection(name: String): C

  private[mongodb] def ensureUniqueIndex(collection: C, indexName: String, fields: (String,Int)*)(implicit ec: ExecutionContext): C

  private[mongodb] def journal(implicit ec: ExecutionContext): C = {
    val journalCollection = collection(journalCollectionName)
    ensureUniqueIndex(journalCollection, journalIndexName,
                      s"$ATOM.${JournallingFieldNames.PROCESSOR_ID}" -> 1,
                      s"$ATOM.$FROM" -> 1,
                      s"$ATOM.$TO" -> 1)
  }

  private[mongodb] def snaps(implicit ec: ExecutionContext): C = {
    val snapsCollection = collection(snapsCollectionName)
    ensureUniqueIndex(snapsCollection, snapsIndexName,
                      SnapshottingFieldNames.PROCESSOR_ID -> 1,
                      SnapshottingFieldNames.SEQUENCE_NUMBER -> -1,
                      TIMESTAMP -> -1)
  }

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

  def deserializeJournal(dbo: D)(implicit ev: CanDeserializeJournal[D]) = ev.deserializeDocument(dbo)
  def serializeJournal(aw: TraversableOnce[Atom])(implicit ev: CanSerializeJournal[D]) = ev.serializeAtom(aw)
}