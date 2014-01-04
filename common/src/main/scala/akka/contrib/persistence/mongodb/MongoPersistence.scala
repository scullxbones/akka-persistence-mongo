package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.serialization.SerializationExtension

object MongoPersistence {

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