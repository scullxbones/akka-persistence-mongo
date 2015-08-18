package akka.contrib.persistence.mongodb

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import com.codahale.metrics.MetricRegistry

import scala.util.Try

object MongoPersistenceExtension extends ExtensionId[MongoPersistenceExtension] {
  
  def lookup = MongoPersistenceExtension

  override def createExtension(actorSystem: ExtendedActorSystem) = {
    val settings = new MongoSettings(actorSystem.settings)
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
  def readJournal: MongoPersistenceReadJournallingApi
  def registry: MetricRegistry = MongoPersistenceDriver.registry
}

class MongoSettings(val systemSettings: ActorSystem.Settings) {


  lazy protected val config = {
    val fullName = s"${getClass.getPackage.getName}.mongo"
    val systemConfig = systemSettings.config
    systemConfig.checkValid(ConfigFactory.defaultReference(), fullName)
    systemConfig.getConfig(fullName)
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

  val JournalCollection = config.getString("journal-collection")
  val JournalIndex = config.getString("journal-index")
  val JournalWriteConcern = config.getString("journal-write-concern")
  val JournalWTimeout = config.getDuration("journal-wtimeout",MILLISECONDS).millis
  val JournalFSync = config.getBoolean("journal-fsync")
  val JournalAutomaticUpgrade = config.getBoolean("journal-automatic-upgrade")

  val SnapsCollection = config.getString("snaps-collection")
  val SnapsIndex = config.getString("snaps-index")
  val SnapsWriteConcern = config.getString("snaps-write-concern")
  val SnapsWTimeout = config.getDuration("snaps-wtimeout",MILLISECONDS).millis
  val SnapsFSync = config.getBoolean("snaps-fsync")


  val Tries = config.getInt("breaker.maxTries")
  val CallTimeout = config.getDuration("breaker.timeout.call", MILLISECONDS).millis
  val ResetTimeout = config.getDuration("breaker.timeout.reset", MILLISECONDS).millis

  val ReadJournalPerFillLimit = config.getInt("journal-read-fill-limit")
}
