package akka.contrib.persistence.mongodb

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtendedActorSystem
import akka.pattern.CircuitBreaker
import akka.actor.ActorSystem
import com.typesafe.config.Config
import scala.concurrent.duration.Duration
import com.typesafe.config.ConfigFactory
import scala.util.Try
import scala.concurrent.Future
import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension
import scala.concurrent.ExecutionContext
import akka.persistence.SelectedSnapshot

object MongoPersistenceExtensionId extends ExtensionId[MongoPersistenceExtension] {
  
  def lookup = MongoPersistenceExtensionId

  override def createExtension(actorSystem: ExtendedActorSystem) = {
	val settings = new MongoSettings(actorSystem.settings, ConfigFactory.load())
	val implementation = settings.Implementation
	Class.forName(implementation).newInstance().asInstanceOf[MongoPersistenceExtension]
  }

  override def get(actorSystem: ActorSystem) = super.get(actorSystem)
}

trait MongoPersistenceBase {
  val actorSystem: ExtendedActorSystem
  
  // Document type
  type D
  // Collection type
  type C
  
  private[mongodb] def collection(name: String)(implicit ec: ExecutionContext): C

  protected lazy val settings = new MongoSettings(actorSystem.settings, ConfigFactory.load())
  protected lazy val mongoUrl = settings.Urls
  protected lazy val mongoDbName = settings.DbName
  
  protected lazy val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)
}

trait MongoPersistenceJournalling extends MongoPersistenceBase {
  private[mongodb] def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext): Future[Option[PersistentRepr]]

  private[mongodb] def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext): Future[Iterator[PersistentRepr]]
  
  private[mongodb] def appendToJournal(persistent: TraversableOnce[PersistentRepr])(implicit ec: ExecutionContext): Future[Unit]

  private[mongodb] def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit]

  private[mongodb] def confirmJournalEntry(pid: String, seq: Long, channelId: String)(implicit ec: ExecutionContext): Future[Unit]
  
  private[mongodb] def replayJournal(pid: String, from: Long, to: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext): Future[Long]

  private[mongodb] def journal(implicit ec: ExecutionContext): C
}

trait MongoPersistenceSnapshotting extends MongoPersistenceBase {
  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext): Future[Option[SelectedSnapshot]]

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext): Future[Unit]
  
  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext): Unit
  
  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext): Unit
  
  private[mongodb] def snaps(implicit ec: ExecutionContext): C
}

trait MongoPersistenceExtension 
	extends Extension 
	with MongoPersistenceJournalling 
	with MongoPersistenceSnapshotting

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
