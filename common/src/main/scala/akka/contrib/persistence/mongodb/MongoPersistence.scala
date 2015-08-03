package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
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
  def serializeAtom(atom: Atom)(implicit serialization: Serialization, system: ActorSystem): D
}

trait CanDeserializeJournal[D] {
  def deserializeDocument(document: D)(implicit serialization: Serialization, system: ActorSystem): Event
}

trait JournalFormats[D] extends CanSerializeJournal[D] with CanDeserializeJournal[D]

abstract class MongoPersistenceDriver(as: ActorSystem) {
  import MongoPersistenceDriver._

  // Collection type
  type C

  // Document type
  type D

  val DEFAULT_DB_NAME = "akka-persistence"

  implicit lazy val actorSystem: ActorSystem = as

  lazy val settings = new MongoSettings(as.settings)

  implicit lazy val serialization = SerializationExtension(actorSystem)
  lazy val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)

  as.registerOnTermination {
    closeConnections()
  }

  private[mongodb] def collection(name: String): C

  private[mongodb] def ensureUniqueIndex(collection: C, indexName: String, fields: (String,Int)*)(implicit ec: ExecutionContext): C

  private[mongodb] def closeConnections(): Unit

  private[mongodb] def upgradeJournalIfNeeded(): Unit

  private[mongodb] lazy val journal: C = {
    val journalCollection = collection(journalCollectionName)
    val indexed = ensureUniqueIndex(journalCollection, journalIndexName,
                      JournallingFieldNames.PROCESSOR_ID -> 1, FROM -> 1, TO -> 1)(concurrent.ExecutionContext.Implicits.global)
    upgradeJournalIfNeeded()
    indexed
  }

  private[mongodb] lazy val snaps: C = {
    val snapsCollection = collection(snapsCollectionName)
    ensureUniqueIndex(snapsCollection, snapsIndexName,
                      SnapshottingFieldNames.PROCESSOR_ID -> 1,
                      SnapshottingFieldNames.SEQUENCE_NUMBER -> -1,
                      TIMESTAMP -> -1)(concurrent.ExecutionContext.Implicits.global)
  }

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

  def deserializeJournal(dbo: D)(implicit ev: CanDeserializeJournal[D]) = ev.deserializeDocument(dbo)
  def serializeJournal(aw: Atom)(implicit ev: CanSerializeJournal[D]) = ev.serializeAtom(aw)
}