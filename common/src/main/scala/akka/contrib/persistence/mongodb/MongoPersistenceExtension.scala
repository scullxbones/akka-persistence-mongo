package akka.contrib.persistence.mongodb

import akka.actor._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.collection.JavaConverters._

object MongoPersistenceExtensionId extends ExtensionId[MongoPersistenceExtension] {
  
  def lookup = MongoPersistenceExtensionId

  override def createExtension(actorSystem: ExtendedActorSystem) = {
	val settings = new MongoSettings(actorSystem.settings, ConfigFactory.load())
	val implementation = settings.Implementation
	Class.forName(implementation).newInstance().asInstanceOf[MongoPersistenceExtension]
  }

  override def get(actorSystem: ActorSystem) = super.get(actorSystem)
}

trait MongoPersistenceExtension extends Extension {
  def journaler: MongoPersistenceJournallingApi
  def snapshotter: MongoPersistenceSnapshottingApi
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
  val SnapsCollection = config.getString("snaps-collection")
  val SnapsIndex = config.getString("snaps-index")

  val Tries = config.getInt("breaker.maxTries")
  val CallTimeout = Duration(config.getMilliseconds("breaker.timeout.call"), MILLISECONDS)
  val ResetTimeout = Duration(config.getMilliseconds("breaker.timeout.reset"), MILLISECONDS)
}
