/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb


import java.util.concurrent.ConcurrentHashMap

import akka.actor._
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

object MongoPersistenceExtension extends ExtensionId[MongoPersistenceExtension] with ExtensionIdProvider {
  
  override def lookup: ExtensionId[MongoPersistenceExtension] = MongoPersistenceExtension

  override def createExtension(actorSystem: ExtendedActorSystem): MongoPersistenceExtension = {
    val settings = MongoSettings(actorSystem.settings)
    val implementation = settings.Implementation
    val implType = actorSystem.dynamicAccess.getClassFor[MongoPersistenceExtension](implementation)
      .getOrElse(Class.forName(implementation, true, Thread.currentThread.getContextClassLoader))
    val implCons = implType.getConstructor(classOf[ActorSystem])
    implCons.newInstance(actorSystem).asInstanceOf[MongoPersistenceExtension]
  }

  override def get(actorSystem: ActorSystem): MongoPersistenceExtension = super.get(actorSystem)
}

trait MongoPersistenceExtension extends Extension {

  private val configuredExtensions = new ConcurrentHashMap[Config, ConfiguredExtension].asScala

  def apply(config: Config): ConfiguredExtension = {
    configuredExtensions.putIfAbsent(config, configured(config))
    configuredExtensions(config)
  }

  def configured(config: Config): ConfiguredExtension

}

trait ConfiguredExtension {
  def journaler: MongoPersistenceJournallingApi
  def snapshotter: MongoPersistenceSnapshottingApi
  def readJournal: MongoPersistenceReadJournallingApi
  def registry: MetricRegistry = DropwizardMetrics.metricRegistry
}

object MongoSettings {
  def apply(systemSettings: ActorSystem.Settings): MongoSettings = {
    val fullName = s"${getClass.getPackage.getName}.mongo"
    val systemConfig = systemSettings.config
    systemConfig.checkValid(ConfigFactory.defaultReference(), fullName)
    new MongoSettings(systemConfig.getConfig(fullName))
  }
}

class MongoSettings(val config: Config) {

  def withOverride(by: Config): MongoSettings = {
    new MongoSettings(by.withFallback(config))
  }

  val Implementation: String = config.getString("driver")

  val MongoUri: String = Try(config.getString("mongouri")).toOption match {
    case Some(uri) => uri
    case None => // Use legacy approach
      val Urls = config.getStringList("urls").asScala.toList.mkString(",")
      val Username = Try(config.getString("username")).toOption
      val Password = Try(config.getString("password")).toOption
      val DbName = config.getString("db")
      (for {
        user <- Username
        password <- Password
      } yield {
        s"mongodb://$user:$password@$Urls/$DbName"
      }) getOrElse s"mongodb://$Urls/$DbName"
  }

  val Database: Option[String] = Try(config.getString("database")).toOption

  val JournalCollection: String = config.getString("journal-collection")
  val JournalIndex: String = config.getString("journal-index")
  val JournalSeqNrIndex: String = config.getString("journal-seq-nr-index")
  val JournalTagIndex: String = config.getString("journal-tag-index")
  val JournalWriteConcern: String = config.getString("journal-write-concern")
  val JournalWTimeout: FiniteDuration = config.getDuration("journal-wtimeout",MILLISECONDS).millis
  val JournalFSync: Boolean = config.getBoolean("journal-fsync")
  val JournalAutomaticUpgrade: Boolean = config.getBoolean("journal-automatic-upgrade")

  val SnapsCollection: String = config.getString("snaps-collection")
  val SnapsIndex: String = config.getString("snaps-index")
  val SnapsWriteConcern: String = config.getString("snaps-write-concern")
  val SnapsWTimeout: FiniteDuration = config.getDuration("snaps-wtimeout",MILLISECONDS).millis
  val SnapsFSync: Boolean = config.getBoolean("snaps-fsync")

  val realtimeEnablePersistence: Boolean = config.getBoolean("realtime-enable-persistence")
  val realtimeCollectionName: String = config.getString("realtime-collection")
  val realtimeCollectionSize: Long = config.getLong("realtime-collection-size")

  val MetadataCollection: String = config.getString("metadata-collection")

  val UseLegacyJournalSerialization: Boolean = config.getBoolean("use-legacy-serialization")
  
  val SuffixBuilderClass: String = config.getString("suffix-builder.class")
  val SuffixSeparator: String = config.getString("suffix-builder.separator")
  val SuffixDropEmptyCollections: Boolean = config.getBoolean("suffix-drop-empty-collections")

  val SuffixMigrationHeavyLoad: Boolean = Option(config.getBoolean("suffix-migration.heavy-load")).getOrElse(false)

  val SuffixMigrationMaxInsertRetry: Int = Option(config.getInt("suffix-migration.max-insert-retry")).filter(_ >= 0).getOrElse(1)
  val SuffixMigrationMaxDeleteRetry: Int = Option(config.getInt("suffix-migration.max-delete-retry")).filter(_ >= 0).getOrElse(1)
  val SuffixMigrationMaxEmptyMetadataRetry: Int = Option(config.getInt("suffix-migration.max-empty-metadata-retry")).filter(_ >= 0).getOrElse(1)

  val SuffixMigrationParallelism: Int = Option(config.getInt("suffix-migration.parallelism")).filter(_ > 0).getOrElse(1)

  val MongoMetricsBuilderClass: String = config.getString("metrics-builder.class")

  val CollectionCache: Config = config.getConfig("collection-cache")
}
