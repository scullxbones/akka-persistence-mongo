/*
 * Copyright (c) 2013-2018 Brian Scully
 * Copyright (c) 2018      Gael Breard, Orange: Optimization, journal collection cache. PR #181
 *
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.contrib.persistence.mongodb.SnapshottingFieldNames._
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.Config
import nl.grons.metrics.scala.InstrumentedBuilder
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

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
  override val metricRegistry: MetricRegistry = MongoPersistenceDriver.registry
}

trait CanSerializeJournal[D] {
  def serializeAtom(atom: Atom): D
}

trait CanDeserializeJournal[D] {
  def deserializeDocument(document: D): Event
}

trait CanSuffixCollectionNames {
  def getSuffixFromPersistenceId(persistenceId: String): String

  def validateMongoCharacters(input: String): String
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

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val actorSystem: ActorSystem = as

  lazy val settings: MongoSettings = {
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

  as.registerOnTermination {
    closeConnections()
  }

  private[mongodb] def collection(name: String): C

  private[mongodb] def cappedCollection(name: String)(implicit ec: ExecutionContext): C

  private[mongodb] def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, fields: (String, Int)*)(implicit ec: ExecutionContext): C => C

  private[mongodb] def closeConnections(): Unit

  private[mongodb] def upgradeJournalIfNeeded(): Unit

  private[mongodb] def upgradeJournalIfNeeded(persistenceId: String): Unit

  /**
    * retrieve suffix from persistenceId
    */
  private[this] def getSuffixFromPersistenceId(persistenceId: String): String = suffixBuilderClassOption match {
    case Some(suffixBuilderClass) if !suffixBuilderClass.trim.isEmpty =>
      val builderClass = Class.forName(suffixBuilderClass)
      val builderCons = builderClass.getConstructor()
      val builderIns = builderCons.newInstance().asInstanceOf[CanSuffixCollectionNames]
      builderIns.getSuffixFromPersistenceId(persistenceId)
    case _ => ""
  }

  /**
    * validate characters in collection name
    */
  private[this] def validateMongoCharacters(input: String): String = suffixBuilderClassOption match {
    case Some(suffixBuilderClass) if !suffixBuilderClass.trim.isEmpty =>
      val builderClass = Class.forName(suffixBuilderClass)
      val builderCons = builderClass.getConstructor()
      val builderIns = builderCons.newInstance().asInstanceOf[CanSuffixCollectionNames]
      builderIns.validateMongoCharacters(input)
    case _ => input
  }

  /**
    * retrieve collection from persistenceId
    */
  private[this] def getSuffixedCollection(persistenceId: String)(build: String => String): C = {
    val name = build(getSuffixFromPersistenceId(persistenceId))
    logger.debug(s"Name used to build collection is $name")
    collection(name)
  }

  /**
    * build name of a collection by appending separator and suffix to usual name in settings
    */
  private[this] def appendSuffixToName(nameInSettings: String)(suffix: String): String = {
    val name =
      suffix match {
        case "" => nameInSettings
        case _ => s"$nameInSettings$suffixSeparator${validateMongoCharacters(suffix)}"
      }
    logger.debug(s"""Suffixed name for value "$nameInSettings" in settings and suffix "$suffix" is "$name"""")
    name
  }

  /**
    * Convenient methods to retrieve journal name from persistenceId
    */
  private[mongodb] def getJournalCollectionName(persistenceId: String): String =
    persistenceId match {
      case "" => journalCollectionName
      case _ => appendSuffixToName(journalCollectionName)(getSuffixFromPersistenceId(persistenceId))
    }

  /**
    * Convenient methods to retrieve snapshot name from persistenceId
    */
  private[mongodb] def getSnapsCollectionName(persistenceId: String): String =
    persistenceId match {
      case "" => snapsCollectionName
      case _ => appendSuffixToName(snapsCollectionName)(getSuffixFromPersistenceId(persistenceId))
    }

  /**
    * Convenient methods to retrieve EXISTING journal collection from persistenceId.
    * CAUTION: this method does NOT create the journal and its indexes.
    */
  private[mongodb] def getJournal(persistenceId: String): C = collection(getJournalCollectionName(persistenceId))

  /**
    * Convenient methods to retrieve EXISTING snapshot collection from persistenceId.
    * CAUTION: this method does NOT create the snapshot and its indexes.
    */
  private[mongodb] def getSnaps(persistenceId: String): C = collection(getSnapsCollectionName(persistenceId))

  private[mongodb] lazy val indexes: Seq[IndexSettings] = Seq(
    IndexSettings(journalIndexName, unique = true, sparse = false, JournallingFieldNames.PROCESSOR_ID -> 1, FROM -> 1, TO -> 1),
    IndexSettings(journalSeqNrIndexName, unique = false, sparse = false, JournallingFieldNames.PROCESSOR_ID -> 1, TO -> -1),
    IndexSettings(journalTagIndexName, unique = false, sparse = true, TAGS -> 1)
  )

  private[mongodb] lazy val journal: C = journal("")

  private[mongodb] val journalMap = TrieMap.empty[String, C]

  private[mongodb] def journal(persistenceId: String): C = {
    val collectionName = getJournalCollectionName(persistenceId)
    journalMap.getOrElseUpdate(collectionName, {
      if (settings.JournalAutomaticUpgrade) {
        logger.debug("Journal automatic upgrade is enabled, executing upgrade process")
        upgradeJournalIfNeeded(persistenceId)
        logger.debug("Journal automatic upgrade process has completed")
      }

      val journalCollection = collection(collectionName)

      indexes.foldLeft(journalCollection) { (acc, index) =>
        import index._
        ensureIndex(name, unique, sparse, fields: _*)(concurrent.ExecutionContext.global)(acc)
      }
    })
  }

  private[mongodb] def removeJournalInCache(persistenceId: String) = {
    val collectionName = getJournalCollectionName(persistenceId)
    journalMap.remove(collectionName)
  }

  private[mongodb] lazy val snaps: C = snaps("")

  private[mongodb] def snaps(persistenceId: String): C = {
    val snapsCollection = collection(getSnapsCollectionName(persistenceId))
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

  // useful in some methods in each driver
  def useSuffixedCollectionNames = suffixBuilderClassOption.isDefined

  def databaseName = settings.Database

  def snapsCollectionName = settings.SnapsCollection

  def snapsIndexName = settings.SnapsIndex

  def snapsWriteSafety: WriteSafety = settings.SnapsWriteConcern

  def snapsWTimeout = settings.SnapsWTimeout

  def snapsFsync = settings.SnapsFSync

  def journalCollectionName = settings.JournalCollection

  def journalIndexName = settings.JournalIndex

  def journalSeqNrIndexName = settings.JournalSeqNrIndex

  def journalTagIndexName = settings.JournalTagIndex

  def journalWriteSafety: WriteSafety = settings.JournalWriteConcern

  def journalWTimeout = settings.JournalWTimeout

  def journalFsync = settings.JournalFSync

  def realtimeEnablePersistence = settings.realtimeEnablePersistence

  def realtimeCollectionName = settings.realtimeCollectionName

  def realtimeCollectionSize = settings.realtimeCollectionSize

  def metadataCollectionName = settings.MetadataCollection

  def mongoUri = settings.MongoUri

  def useLegacySerialization = settings.UseLegacyJournalSerialization

  def suffixBuilderClassOption = Option(settings.SuffixBuilderClass).filter(_.trim.nonEmpty)

  def suffixSeparator = settings.SuffixSeparator match {
    case str if !str.isEmpty => validateMongoCharacters(settings.SuffixSeparator).substring(0, 1)
    case _ => "_"
  }

  def suffixDropEmpty = settings.SuffixDropEmptyCollections

  def deserializeJournal(dbo: D)(implicit ev: CanDeserializeJournal[D]) = ev.deserializeDocument(dbo)

  def serializeJournal(aw: Atom)(implicit ev: CanSerializeJournal[D]) = ev.serializeAtom(aw)
}