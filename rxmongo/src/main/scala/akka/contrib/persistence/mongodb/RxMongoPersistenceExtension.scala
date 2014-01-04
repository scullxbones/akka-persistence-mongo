package akka.contrib.persistence.mongodb

import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson._
import reactivemongo.bson.buffer.ArrayReadableBuffer

import akka.actor.ActorSystem

object RxMongoPersistenceExtension {
  implicit object BsonBinaryHandler extends BSONHandler[BSONBinary, Array[Byte]] {
    def read(bson: reactivemongo.bson.BSONBinary): Array[Byte] =
      bson.as[Array[Byte]]

    def write(t: Array[Byte]): reactivemongo.bson.BSONBinary =
      BSONBinary(ArrayReadableBuffer(t), Subtype.GenericBinarySubtype)
  }
}

trait RxMongoPersistenceDriver extends MongoPersistenceDriver with MongoPersistenceBase {

  // Collection type
  type C = BSONCollection

  private[this] lazy val driver = MongoDriver(actorSystem)
  private[this] lazy val connection = driver.connection(mongoUrl)
  private[this] lazy val db = connection(mongoDbName)(actorSystem.dispatcher)

  private[mongodb] override def collection(name: String) = db[BSONCollection](name)
}

class RxMongoDriver(val actorSystem: ActorSystem) extends RxMongoPersistenceDriver

class RxMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension {

  private[this] lazy val driver = new RxMongoDriver(actorSystem)
  private[this] lazy val _journaler = new RxMongoJournaller(driver)
  private[this] lazy val _snapshotter = new RxMongoSnapshotter(driver)

  override def journaler = _journaler
  override def snapshotter = _snapshotter
} 
