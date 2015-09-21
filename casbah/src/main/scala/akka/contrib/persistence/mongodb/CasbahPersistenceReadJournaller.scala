package akka.contrib.persistence.mongodb

import akka.actor.Props
import akka.persistence.query.{EventEnvelope, Hint}
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._

object AllPersistenceIds {
  def props(driver: CasbahMongoDriver): Props = Props(new AllPersistenceIds(driver))
}

class AllPersistenceIds(val driver: CasbahMongoDriver) extends SyncActorPublisher[String, Stream[String]] {
  import CasbahSerializers._

  override protected def initialCursor: Stream[String] =
    driver.journal
          .distinct(PROCESSOR_ID, MongoDBObject())
          .toStream
          .collect { case s:String => s }

  override protected def next(c: Stream[String], atMost: Long): (Vector[String], Stream[String]) = {
    val (buf,remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[String]): Boolean = {
    c.isEmpty
  }

  override protected def discard(c: Stream[String]): Unit = ()
}

object AllEvents {
  def props(driver: CasbahMongoDriver): Props = Props(new AllEvents(driver))
}

class AllEvents(val driver: CasbahMongoDriver) extends SyncActorPublisher[EventEnvelope, Stream[EventEnvelope]] {
  import CasbahSerializers._

  override protected def initialCursor: Stream[EventEnvelope] =
    driver.journal
          .find(MongoDBObject())
          .sort(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1))
          .toStream
          .flatMap(_.getAs[MongoDBList](EVENTS))
          .flatMap(lst => lst.collect {case x:DBObject => x} )
          .map(driver.deserializeJournal)
          .zipWithIndex
          .map { case(e,i) => e.toEnvelope(i) }

  override protected def next(c: Stream[EventEnvelope], atMost: Long): (Vector[EventEnvelope], Stream[EventEnvelope]) = {
    val (buf,remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[EventEnvelope]): Boolean = c.isEmpty

  override protected def discard(c: Stream[EventEnvelope]): Unit = ()
}

object EventsByPersistenceId {
  def props(driver: CasbahMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long): Props =
    Props(new EventsByPersistenceId(driver, persistenceId, fromSeq, toSeq))
}

class EventsByPersistenceId(val driver: CasbahMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long) extends SyncActorPublisher[EventEnvelope, Stream[EventEnvelope]] {
  import CasbahSerializers._

  override protected def initialCursor: Stream[EventEnvelope] =
    driver.journal
      .find((PROCESSOR_ID $eq persistenceId) ++ (FROM $gte fromSeq) ++ (FROM $lte toSeq))
      .sort(MongoDBObject(PROCESSOR_ID -> 1, FROM -> 1))
      .toStream
      .flatMap(_.getAs[MongoDBList](EVENTS))
      .flatMap(lst => lst.collect {case x:DBObject => x} )
      .filter(dbo => dbo.getAs[Long](SEQUENCE_NUMBER).exists(sn => sn >= fromSeq && sn <= toSeq))
      .map(driver.deserializeJournal)
      .zipWithIndex
      .map { case(e,i) => e.toEnvelope(i) }

  override protected def next(c: Stream[EventEnvelope], atMost: Long): (Vector[EventEnvelope], Stream[EventEnvelope]) = {
    val (buf,remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[EventEnvelope]): Boolean = c.isEmpty

  override protected def discard(c: Stream[EventEnvelope]): Unit = ()
}

class CasbahPersistenceReadJournaller(driver: CasbahMongoDriver) extends MongoPersistenceReadJournallingApi {
  override def allPersistenceIds(hints: Hint*): Props = AllPersistenceIds.props(driver)

  override def allEvents(hints: Hint*): Props = AllEvents.props(driver)

  override def eventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long, hints: Hint*): Props =
    EventsByPersistenceId.props(driver,persistenceId,fromSeq,toSeq)
}
