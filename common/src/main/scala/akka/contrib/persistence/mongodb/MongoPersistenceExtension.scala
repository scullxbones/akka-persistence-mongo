package akka.contrib.persistence.mongodb

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import com.codahale.metrics.MetricRegistry

object MongoPersistenceExtensionId extends ExtensionId[MongoPersistenceExtension] {
  
  def lookup = MongoPersistenceExtensionId

  override def createExtension(actorSystem: ExtendedActorSystem) = {
	val settings = new MongoSettings(actorSystem.settings, ConfigFactory.load())
	val implementation = settings.Implementation
	val implType = Class.forName(implementation)
	val implCons = implType.getConstructor(classOf[ActorSystem])
	implCons.newInstance(actorSystem).asInstanceOf[MongoPersistenceExtension]
  }

  override def get(actorSystem: ActorSystem) = super.get(actorSystem)
}

trait MongoPersistenceExtension extends Extension {
  def journaler: MongoPersistenceJournallingApi
  def snapshotter: MongoPersistenceSnapshottingApi
  def registry: MetricRegistry = MongoPersistenceBase.registry
}

class MongoSettings(override val systemSettings: ActorSystem.Settings, override val userConfig: Config)
  extends UserOverrideSettings(systemSettings, userConfig) {

  protected override val name = "mongo"
  
  val _config = config  
  
  val Implementation = config.getString("driver")
  
  val Urls = config.getStringList("urls").asScala.toList
  val DbName = config.getString("db")
  val JournalCollection = config.getString("journal-collection")
  val JournalIndex = config.getString("journal-index")
  val JournalWriteConcern = config.getString("journal-write-concern")
  val SnapsCollection = config.getString("snaps-collection")
  val SnapsIndex = config.getString("snaps-index")
  val SnapsWriteConcern = config.getString("snaps-write-concern")

  val Tries = config.getInt("breaker.maxTries")
  val CallTimeout = config.getDuration("breaker.timeout.call", MILLISECONDS).millis
  val ResetTimeout = config.getDuration("breaker.timeout.reset", MILLISECONDS).millis
}
