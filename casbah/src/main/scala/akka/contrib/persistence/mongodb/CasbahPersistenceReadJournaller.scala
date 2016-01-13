package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, Props}
import com.mongodb.casbah.Imports._
import com.mongodb.{Bytes, DBObject}

import scala.concurrent.Future

object CurrentAllPersistenceIds {
  def props(driver: CasbahMongoDriver): Props = Props(new CurrentAllPersistenceIds(driver))
}

class CurrentAllPersistenceIds(val driver: CasbahMongoDriver) extends SyncActorPublisher[String, Stream[String]] {
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

object CurrentAllEvents {
  def props(driver: CasbahMongoDriver): Props = Props(new CurrentAllEvents(driver))
}

class CurrentAllEvents(val driver: CasbahMongoDriver) extends SyncActorPublisher[Event, Stream[Event]] {
  import CasbahSerializers._

  override protected def initialCursor: Stream[Event] =
    driver.journal
          .find(MongoDBObject())
          .sort(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1))
          .toStream
          .flatMap(_.getAs[MongoDBList](EVENTS))
          .flatMap(lst => lst.collect {case x:DBObject => x} )
          .map(driver.deserializeJournal)

  override protected def next(c: Stream[Event], atMost: Long): (Vector[Event], Stream[Event]) = {
    val (buf,remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[Event]): Boolean = c.isEmpty

  override protected def discard(c: Stream[Event]): Unit = ()
}

object CurrentEventsByPersistenceId {
  def props(driver: CasbahMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long): Props =
    Props(new CurrentEventsByPersistenceId(driver, persistenceId, fromSeq, toSeq))
}

class CurrentEventsByPersistenceId(val driver: CasbahMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long) extends SyncActorPublisher[Event, Stream[Event]] {
  import CasbahSerializers._

  override protected def initialCursor: Stream[Event] =
    driver.journal
      .find((PROCESSOR_ID $eq persistenceId) ++ (FROM $lte toSeq) ++ (TO $gte fromSeq))
      .sort(MongoDBObject(PROCESSOR_ID -> 1, FROM -> 1))
      .toStream
      .flatMap(_.getAs[MongoDBList](EVENTS))
      .flatMap(lst => lst.collect {case x:DBObject => x} )
      .filter(dbo => dbo.getAs[Long](SEQUENCE_NUMBER).exists(sn => sn >= fromSeq && sn <= toSeq))
      .map(driver.deserializeJournal)

  override protected def next(c: Stream[Event], atMost: Long): (Vector[Event], Stream[Event]) = {
    val (buf,remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[Event]): Boolean = c.isEmpty

  override protected def discard(c: Stream[Event]): Unit = ()
}

class CasbahMongoJournalStream(val driver: CasbahMongoDriver) extends JournalStream[MongoCollection]{
  import CasbahSerializers._

  override def cursor() = {
    val c = driver.realtime
    c.addOption(Bytes.QUERYOPTION_TAILABLE)
    c.addOption(Bytes.QUERYOPTION_AWAITDATA)
    c
  }

  override def publishEvents() = {
    implicit val ec = driver.querySideDispatcher
    Future {
      cursor().foreach { next =>
        if (next.keySet().contains(EVENTS)) {
          val events = next.as[MongoDBList](EVENTS).collect { case x: DBObject => driver.deserializeJournal(x) }
          events.foreach(driver.actorSystem.eventStream.publish)
        }
      }
    }
    ()
  }
}

class CasbahPersistenceReadJournaller(driver: CasbahMongoDriver) extends MongoPersistenceReadJournallingApi {

  private val journalStreaming = {
    val stream = new CasbahMongoJournalStream(driver)
    stream.publishEvents()
    stream
  }

  override def currentAllEvents: Props = CurrentAllEvents.props(driver)

  override def currentPersistenceIds: Props = CurrentAllPersistenceIds.props(driver)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long): Props =
    CurrentEventsByPersistenceId.props(driver,persistenceId,fromSeq,toSeq)

  override def subscribeJournalEvents(subscriber: ActorRef): Unit = {
    driver.actorSystem.eventStream.subscribe(subscriber, classOf[Event])
    ()
  }
}
