package akka.contrib.persistence.mongodb

import akka.actor._
import akka.pattern.CircuitBreaker
import akka.persistence._
import akka.serialization.SerializationExtension

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.util.Try
import scala.concurrent._
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

trait MongoPersistenceDriver {
  // Collection type
  type C
  
  private[mongodb] def collection(name: String): C
}

trait MongoPersistenceBase {
  val actorSystem: ActorSystem
  
  private[this] lazy val settings = new MongoSettings(actorSystem.settings, ConfigFactory.load())
  
  def snapsCollectionName = settings.SnapsCollection
  def snapsIndexName = settings.SnapsIndex
  def journalCollectionName = settings.JournalCollection
  def journalIndexName = settings.JournalIndex
  def mongoUrl = settings.Urls
  def mongoDbName = settings.DbName
  
  lazy val serialization = actorSystem.extension(SerializationExtension)
  lazy val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)
}

object JournallingFieldNames {
  final val PROCESSOR_ID = "pid"
  final val SEQUENCE_NUMBER = "sn"
  final val CONFIRMS = "cs"
  final val DELETED = "dl"
  final val SERIALIZED = "pr"
}

trait MongoPersistenceJournallingApi{
  private[mongodb] def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext): Future[Option[PersistentRepr]]

  private[mongodb] def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext): Future[Iterator[PersistentRepr]]
  
  private[mongodb] def appendToJournal(persistent: TraversableOnce[PersistentRepr])(implicit ec: ExecutionContext): Future[Unit]

  private[mongodb] def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit]

  private[mongodb] def confirmJournalEntry(pid: String, seq: Long, channelId: String)(implicit ec: ExecutionContext): Future[Unit]
  
  private[mongodb] def replayJournal(pid: String, from: Long, to: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext): Future[Long]
}

object SnapshottingFieldNames {
  final val PROCESSOR_ID = "pid"
  final val SEQUENCE_NUMBER = "sn"
  final val TIMESTAMP = "ts"
  final val SERIALIZED = "ss"
}

trait MongoPersistenceSnapshottingApi {
  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext): Future[Option[SelectedSnapshot]]

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext): Future[Unit]
  
  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext): Unit
  
  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext): Unit
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
