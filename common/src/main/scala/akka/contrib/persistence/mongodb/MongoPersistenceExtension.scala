package akka.contrib.persistence.mongodb


import java.util.concurrent.ConcurrentHashMap

import akka.actor._
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

object MongoPersistenceExtension extends ExtensionId[MongoPersistenceExtension] with ExtensionIdProvider {
  
  override def lookup = MongoPersistenceExtension

  override def createExtension(actorSystem: ExtendedActorSystem) = {
    val settings = MongoSettings(actorSystem.settings)
    val implementation = settings.Implementation
    val implType = Class.forName(implementation)
    val implCons = implType.getConstructor(classOf[ActorSystem])
    implCons.newInstance(actorSystem).asInstanceOf[MongoPersistenceExtension]
  }

  override def get(actorSystem: ActorSystem) = super.get(actorSystem)
}

trait MongoPersistenceExtension extends Extension {

  private val configuredExtensions = new ConcurrentHashMap[Config, ConfiguredExtension].asScala

  def apply(config: Config): ConfiguredExtension = {
    configuredExtensions.putIfAbsent(config, configured(config))
    configuredExtensions.get(config).get
  }

  def configured(config: Config): ConfiguredExtension

}

trait ConfiguredExtension {
  def journaler: MongoPersistenceJournallingApi
  def snapshotter: MongoPersistenceSnapshottingApi
  def readJournal: MongoPersistenceReadJournallingApi
  def registry: MetricRegistry = MongoPersistenceDriver.registry
}

object MongoSettings {
  def apply(systemSettings: ActorSystem.Settings) = {
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

  val Implementation = config.getString("driver")

  val MongoUri = Try(config.getString("mongouri")).toOption match {
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

  val Database = Try(config.getString("database")).toOption

  val JournalCollection = config.getString("journal-collection")
  val JournalIndex = config.getString("journal-index")
  val JournalSeqNrIndex = config.getString("journal-seq-nr-index")
  val JournalWriteConcern = config.getString("journal-write-concern")
  val JournalWTimeout = config.getDuration("journal-wtimeout",MILLISECONDS).millis
  val JournalFSync = config.getBoolean("journal-fsync")
  val JournalAutomaticUpgrade = config.getBoolean("journal-automatic-upgrade")

  val SnapsCollection = config.getString("snaps-collection")
  val SnapsIndex = config.getString("snaps-index")
  val SnapsWriteConcern = config.getString("snaps-write-concern")
  val SnapsWTimeout = config.getDuration("snaps-wtimeout",MILLISECONDS).millis
  val SnapsFSync = config.getBoolean("snaps-fsync")

  val realtimeEnablePersistence = config.getBoolean("realtime-enable-persistence")
  val realtimeCollectionName = config.getString("realtime-collection")
  val realtimeCollectionSize = config.getLong("realtime-collection-size")

  val MetadataCollection = config.getString("metadata-collection")

  val Tries = config.getInt("breaker.maxTries")
  val CallTimeout = config.getDuration("breaker.timeout.call", MILLISECONDS).millis
  val ResetTimeout = config.getDuration("breaker.timeout.reset", MILLISECONDS).millis

  val UseLegacyJournalSerialization = config.getBoolean("use-legacy-serialization")
}
