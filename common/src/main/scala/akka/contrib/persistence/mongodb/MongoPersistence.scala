/*
 * Copyright (c) 2013-2018 Brian Scully
 * Copyright (c) 2018      Gael Breard, Orange: Optimization, journal collection cache. PR #181
 *
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * Florian FENDT: optimization, solution collection cache
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.contrib.persistence.mongodb.SnapshottingFieldNames._
import akka.stream.Materializer
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
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

abstract class MongoPersistenceDriver(as: ActorSystem, config: Config)
  extends WithMongoPersistencePluginDispatcher(as, config) {

  import MongoPersistenceDriver._

  // Collection type
  type C

  // Document type
  type D

  val DEFAULT_DB_NAME = "akka-persistence"

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val actorSystem: ActorSystem = as
  implicit val materializer: Materializer = Materializer(as)

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

  def collection(name: String): Future[C]

  def collectionNames: Future[List[String]]

  def ensureCollection(name: String): Future[C]

  def cappedCollection(name: String): Future[C]

  def ensureIndex(indexName: String, unique: Boolean, sparse: Boolean, fields: (String, Int)*): C => Future[C]

  def removeEmptyCollection(collection: C, indexName: String): Future[Unit]

  def closeConnections(): Unit

  def getMongoVersionFromBuildInfo: Future[String]

  private[mongodb] lazy val mongoVersion: Future[ServerVersion] = {
    getMongoVersionFromBuildInfo.map {
      case ServerVersion(v) => v
      case _ =>
        ServerVersion.Unsupported(-1.0,"UNKNOWN")
    }
  }

  def getAllCollectionsAsFuture(nameFilter: Option[String => Boolean]): Future[List[C]] = {
    def excluded(name: String): Boolean =
      name == metadataCollectionName ||
        name.startsWith("system.")

    def allPass(name: String): Boolean = true

    for {
      names     <- collectionNames
      list      <- Future.sequence(names.filterNot(excluded).filter(nameFilter.getOrElse(allPass)).map(collection))
    } yield list
  }

  def getCollectionsAsFuture(collectionName: String): Future[List[C]] =
    getAllCollectionsAsFuture(Option(_.startsWith(collectionName)))

  def getJournalCollections: Future[List[C]] =
    getCollectionsAsFuture(journalCollectionName)

  def journalCollectionsAsFuture: Future[List[C]] =
    getCollectionsAsFuture(journalCollectionName)

  def getSnapshotCollections: Future[List[C]] =
    getCollectionsAsFuture(snapsCollectionName)

  def snapshotCollectionsAsFuture: Future[List[C]] =
    getCollectionsAsFuture(snapsCollectionName)

  def removeEmptyJournal(jnl: C): Future[Unit] =
    removeEmptyCollection(jnl, journalIndexName)

  def removeEmptySnapshot(snp: C): Future[Unit] =
    removeEmptyCollection(snp, snapsIndexName)

  private val canSuffixCollectionNamesBuilder: Option[CanSuffixCollectionNames] = suffixBuilderClassOption match {
    case Some(suffixBuilderClass) if !suffixBuilderClass.trim.isEmpty =>
      val reflectiveAccess = ReflectiveLookupExtension(actorSystem)
      val builderClass = reflectiveAccess.unsafeReflectClassByName[CanSuffixCollectionNames](suffixBuilderClass)
      val builderCons = builderClass.getConstructor()
      Some(builderCons.newInstance())
    case _ => None
  }

  /**
    * retrieve suffix from persistenceId
    */
  private[this] def getSuffixFromPersistenceId(persistenceId: String): String = canSuffixCollectionNamesBuilder match {
    case Some(builderIns) => builderIns.getSuffixFromPersistenceId(persistenceId)
    case _ => ""
  }

  /**
    * validate characters in collection name
    */
  private[this] def validateMongoCharacters(input: String): String = canSuffixCollectionNamesBuilder match {
    case Some(builderIns) => builderIns.validateMongoCharacters(input)
    case _ => input
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
  def getJournalCollectionName(persistenceId: String): String =
    persistenceId match {
      case "" => journalCollectionName
      case _ => appendSuffixToName(journalCollectionName)(getSuffixFromPersistenceId(persistenceId))
    }

  /**
    * Convenient methods to retrieve snapshot name from persistenceId
    */
  def getSnapsCollectionName(persistenceId: String): String =
    persistenceId match {
      case "" => snapsCollectionName
      case _ => appendSuffixToName(snapsCollectionName)(getSuffixFromPersistenceId(persistenceId))
    }

  /**
    * Convenient methods to retrieve EXISTING journal collection from persistenceId.
    * CAUTION: this method does NOT create the journal and its indexes.
    */
  def getJournal(persistenceId: String): Future[C] = collection(getJournalCollectionName(persistenceId))

  /**
    * Convenient methods to retrieve EXISTING snapshot collection from persistenceId.
    * CAUTION: this method does NOT create the snapshot and its indexes.
    */
  def getSnaps(persistenceId: String): Future[C] = collection(getSnapsCollectionName(persistenceId))

  private[mongodb] lazy val indexes: Seq[IndexSettings] = Seq(
    IndexSettings(journalIndexName, unique = true, sparse = false, JournallingFieldNames.PROCESSOR_ID -> 1, FROM -> 1, TO -> 1),
    IndexSettings(journalSeqNrIndexName, unique = false, sparse = false, JournallingFieldNames.PROCESSOR_ID -> 1, TO -> -1),
    IndexSettings(journalTagIndexName, unique = false, sparse = true, TAGS -> 1)
  )

  private[this] val journalCache = MongoCollectionCache[Future[C]](settings.CollectionCache, "journal", actorSystem)

  def journal: Future[C] = journal("")

  def journal(persistenceId: String): Future[C] = {
    val collectionName = getJournalCollectionName(persistenceId)

    journalCache.getOrElseCreate(collectionName, theCollectionName => {
      val journalCollection = ensureCollection(theCollectionName)

      indexes.foldLeft(journalCollection) { (acc, index) =>
        import index._
        acc.flatMap(ensureIndex(name, unique, sparse, fields: _*)(_))
      }
    })
  }

  def removeJournalInCache(persistenceId: String): Unit = {
    val collectionName = getJournalCollectionName(persistenceId)
    journalCache.invalidate(collectionName)
  }

  private[this] val snapsCache = MongoCollectionCache[Future[C]](settings.CollectionCache, "snaps", actorSystem)

  def snaps: Future[C] = snaps("")

  def snaps(persistenceId: String): Future[C] = {
    val collectionName = getSnapsCollectionName(persistenceId)
    snapsCache.getOrElseCreate(collectionName, theCollectionName => {
      val snapsCollection = ensureCollection(theCollectionName)

      snapsCollection.flatMap(
        ensureIndex(snapsIndexName, unique = true, sparse = false,
          SnapshottingFieldNames.PROCESSOR_ID -> 1,
          SnapshottingFieldNames.SEQUENCE_NUMBER -> -1,
          TIMESTAMP -> -1)(_)
      )
    })
  }

  def removeSnapsInCache(persistenceId: String): Unit = {
    val collectionName = getSnapsCollectionName(persistenceId)
    snapsCache.invalidate(collectionName)
  }

  private[this] val realtimeCache = MongoCollectionCache[Future[C]](settings.CollectionCache, "realtime", actorSystem)

  private[mongodb] def realtime: Future[C] =
    realtimeCache.getOrElseCreate(realtimeCollectionName, collectionName => cappedCollection(collectionName))

  private[mongodb] val querySideDispatcher = actorSystem.dispatchers.lookup("akka-contrib-persistence-query-dispatcher")

  private[this] val metadataCache = MongoCollectionCache[Future[C]](settings.CollectionCache, "metadata", actorSystem)

  def metadata: Future[C] =
    metadataCache.getOrElseCreate(metadataCollectionName, collectionName => {
      val metadataCollection = ensureCollection(collectionName)
      metadataCollection.flatMap(
        ensureIndex("akka_persistence_metadata_pid",
          unique = true, sparse = true,
          JournallingFieldNames.PROCESSOR_ID -> 1)(_)
      )
    })

  // useful in some methods in each driver
  def useSuffixedCollectionNames: Boolean = suffixBuilderClassOption.isDefined

  def databaseName: Option[String] = settings.Database

  def snapsCollectionName: String = settings.SnapsCollection

  def snapsIndexName: String = settings.SnapsIndex

  def snapsWriteSafety: WriteSafety = settings.SnapsWriteConcern

  def snapsWTimeout: FiniteDuration = settings.SnapsWTimeout

  def snapsFsync: Boolean = settings.SnapsFSync

  def journalCollectionName: String = settings.JournalCollection

  def journalIndexName: String = settings.JournalIndex

  def journalSeqNrIndexName: String = settings.JournalSeqNrIndex

  def journalTagIndexName: String = settings.JournalTagIndex

  def journalWriteSafety: WriteSafety = settings.JournalWriteConcern

  def journalWTimeout: FiniteDuration = settings.JournalWTimeout

  def journalFsync: Boolean = settings.JournalFSync

  def realtimeEnablePersistence: Boolean = settings.realtimeEnablePersistence

  def realtimeCollectionName: String = settings.realtimeCollectionName

  def realtimeCollectionSize: Long = settings.realtimeCollectionSize

  def metadataCollectionName: String = settings.MetadataCollection

  def mongoUri: String = settings.MongoUri

  def useLegacySerialization: Boolean = settings.UseLegacyJournalSerialization

  def suffixBuilderClassOption: Option[String] = Option(settings.SuffixBuilderClass).filter(_.trim.nonEmpty)

  def suffixSeparator: String = settings.SuffixSeparator match {
    case str if !str.isEmpty => validateMongoCharacters(settings.SuffixSeparator).substring(0, 1)
    case _ => "_"
  }

  def suffixDropEmpty: Boolean = settings.SuffixDropEmptyCollections

  def deserializeJournal(dbo: D)(implicit ev: CanDeserializeJournal[D]): Event = ev.deserializeDocument(dbo)

  def serializeJournal(aw: Atom)(implicit ev: CanSerializeJournal[D]): D = ev.serializeAtom(aw)
}