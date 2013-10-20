package com.github.scullxbones.mongka

import scala.concurrent.ExecutionContext.Implicits._
import scala.collection.JavaConversions._
import scala.concurrent.duration._

import akka.actor._
import akka.serialization._
import reactivemongo.bson._
import reactivemongo.api._
import reactivemongo.bson.buffer.ArrayReadableBuffer
import reactivemongo.api.indexes.Index
import reactivemongo.api.indexes.IndexType
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.pattern.CircuitBreaker

object serializers {

  implicit object BsonBinaryHandler extends BSONHandler[BSONBinary, Array[Byte]] {
    def read(bson: reactivemongo.bson.BSONBinary): Array[Byte] =
      bson.as[Array[Byte]]

    def write(t: Array[Byte]): reactivemongo.bson.BSONBinary =
      BSONBinary(ArrayReadableBuffer(t), Subtype.GenericBinarySubtype)
  }
}

class MongkaSettings(override val systemSettings: ActorSystem.Settings, override val userConfig: Config)
  extends UserOverrideSettings(systemSettings, userConfig) {

  protected override val name = "mongo"

  val Urls = config.getStringList("urls")
  val DbName = config.getString("db")
  val JournalCollection = config.getString("journal-collection")
  val JournalIndex = config.getString("journal-index")
  val SnapsCollection = config.getString("snaps-collection")
  val SnapsIndex = config.getString("snaps-index")

  val Tries = config.getInt("breaker.maxTries")
  val CallTimeout = Duration(config.getMilliseconds("breaker.timeout.call"), MILLISECONDS)
  val ResetTimeout = Duration(config.getMilliseconds("breaker.timeout.reset"), MILLISECONDS)

}

class MongkaExtension(actorSystem: ExtendedActorSystem) extends Extension {

  private[this] val settings = new MongkaSettings(actorSystem.settings, ConfigFactory.load())

  val mongoUrl = settings.Urls
  val mongoDbName = settings.DbName

  lazy val driver = new MongoDriver

  lazy val connection = driver.connection(mongoUrl)
  lazy val db = connection(mongoDbName)

  lazy val journal = { 
    val journal = db(settings.JournalCollection)
    journal.indexesManager.ensure(new Index(
      key = Seq(("pid", IndexType.Ascending), ("sq", IndexType.Ascending), ("dl", IndexType.Ascending)),
      background = true,
      unique = true,
      name = Some(settings.JournalIndex)))
    journal
  }

  lazy val snaps = {
    val snaps = db(settings.SnapsCollection)
    snaps.indexesManager.ensure(new Index(
      key = Seq(("pid", IndexType.Ascending), ("sq", IndexType.Descending), ("ts", IndexType.Descending)),
      background = true,
      unique = true,
      name = Some(settings.SnapsIndex)))
    snaps
  }

  val breaker = CircuitBreaker(actorSystem.scheduler, settings.Tries, settings.CallTimeout, settings.ResetTimeout)

  def collection(name: String) = db(name)
}

object MongkaExtensionId extends ExtensionId[MongkaExtension] {

  def lookup = MongkaExtensionId

  override def createExtension(actorSystem: ExtendedActorSystem) =
    new MongkaExtension(actorSystem)

  override def get(actorSystem: ActorSystem) = super.get(actorSystem)
}


