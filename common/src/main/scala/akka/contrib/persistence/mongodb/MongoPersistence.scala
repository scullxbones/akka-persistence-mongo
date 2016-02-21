package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.contrib.persistence.mongodb.SnapshottingFieldNames._
import akka.pattern.CircuitBreaker
import akka.serialization.{Serialization, SerializationExtension}
import com.codahale.metrics.SharedMetricRegistries
import com.typesafe.config.Config
import nl.grons.metrics.scala.InstrumentedBuilder
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.util.{Success, Failure, Try}

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
  
  private[mongodb] val registry = SharedMetricRegistries.getOrCreate("mongodb")
}

trait Instrumented extends InstrumentedBuilder {
  override val metricRegistry = MongoPersistenceDriver.registry
}

trait CanSerializeJournal[D] {
  def serializeAtom(atom: Atom)(implicit serialization: Serialization, system: ActorSystem): D
}

trait CanDeserializeJournal[D] {
  def deserializeDocument(document: D)(implicit serialization: Serialization, system: ActorSystem): Event
}

trait JournalFormats[D] extends CanSerializeJournal[D] with CanDeserializeJournal[D]

private case class IndexSettings(name: String, unique: Boolean, sparse: Boolean, fields: (String, Int)*)

abstract class MongoPersistenceDriver(as: ActorSystem, config: Config) {
  import MongoPersistenceDriver._

  // Collection type
  type C

  // Document type
  type D

  val DEFAULT_DB_NAME = "akka-persistence"

  protected val logger = LoggerFactory.getLogger(getClass)

  implicit lazy val actorSystem: ActorSystem = as

  lazy val settings = {
    val defaults = MongoSettings(as.settings)
    Try(config.getConfig("overrides")) match {
      case Success(overrides) =>
        logger.info("Applying configuration-specific overrides for driver")
        defaults.withOverride(overrides)
      case Failure(_) =>
        logger.debug("No configuration-specific overrides found to apply to driver")
        defaults
    }
  }

  lazy val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)

  as.registerOnTermination {
    closeConnections()
  }

  private[mongodb] def collection(name: String): C

  private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext): C

  private[mongodb] def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, fields: (String,Int)*)(implicit ec: ExecutionContext): C => C

  private[mongodb] def closeConnections(): Unit

  private[mongodb] def upgradeJournalIfNeeded(): Unit

  private[mongodb] lazy val indexes: Seq[IndexSettings] = Seq(
    IndexSettings(journalIndexName, unique = true, sparse = false, JournallingFieldNames.PROCESSOR_ID -> 1, FROM -> 1, TO -> 1),
    IndexSettings(journalSeqNrIndexName, unique = false, sparse = false, JournallingFieldNames.PROCESSOR_ID -> 1, TO -> -1)
  )

  private[mongodb] lazy val journal: C = {
    if (settings.JournalAutomaticUpgrade) {
      logger.debug("Journal automatic upgrade is enabled, executing upgrade process")
      upgradeJournalIfNeeded()
      logger.debug("Journal automatic upgrade process has completed")
    }
    val journalCollection = collection(journalCollectionName)

    indexes.foldLeft(journalCollection) { (acc, index) =>
      import index._
      ensureIndex(name, unique, sparse, fields:_*)(concurrent.ExecutionContext.global)(acc)
    }
  }

  private[mongodb] lazy val snaps: C = {
    val snapsCollection = collection(snapsCollectionName)
    ensureIndex(snapsIndexName, unique = true, sparse = false,
                SnapshottingFieldNames.PROCESSOR_ID -> 1,
                SnapshottingFieldNames.SEQUENCE_NUMBER -> -1,
                TIMESTAMP -> -1)(concurrent.ExecutionContext.global)(snapsCollection)
  }
  private[mongodb] lazy val realtime: C = {
    cappedCollection(realtimeCollectionName)(concurrent.ExecutionContext.global)
  }
  private[mongodb] val querySideDispatcher = actorSystem.dispatchers.lookup("akka-contrib-persistence-query-dispatcher")

  private[mongodb] lazy val metadata: C = {
    val metadataCollection = collection(metadataCollectionName)
    ensureIndex("akka_persistence_metadata_pid",
                unique = true, sparse = true,
                JournallingFieldNames.PROCESSOR_ID -> 1)(concurrent.ExecutionContext.global)(metadataCollection)
  }

  def databaseName = settings.Database
  def snapsCollectionName = settings.SnapsCollection
  def snapsIndexName = settings.SnapsIndex
  def snapsWriteSafety: WriteSafety = settings.SnapsWriteConcern
  def snapsWTimeout = settings.SnapsWTimeout
  def snapsFsync = settings.SnapsFSync
  def journalCollectionName = settings.JournalCollection
  def journalIndexName = settings.JournalIndex
  def journalSeqNrIndexName = settings.JournalSeqNrIndex
  def journalWriteSafety: WriteSafety = settings.JournalWriteConcern
  def journalWTimeout = settings.JournalWTimeout
  def journalFsync = settings.JournalFSync
  def realtimeEnablePersistence = settings.realtimeEnablePersistence
  def realtimeCollectionName = settings.realtimeCollectionName
  def realtimeCollectionSize = settings.realtimeCollectionSize
  def metadataCollectionName = settings.MetadataCollection
  def mongoUri = settings.MongoUri
  def useLegacySerialization = settings.UseLegacyJournalSerialization

  implicit def serialization = SerializationExtension(actorSystem)
  def deserializeJournal(dbo: D)(implicit ev: CanDeserializeJournal[D]) = ev.deserializeDocument(dbo)(serialization, actorSystem)
  def serializeJournal(aw: Atom)(implicit ev: CanSerializeJournal[D]) = ev.serializeAtom(aw)(serialization, actorSystem)
}