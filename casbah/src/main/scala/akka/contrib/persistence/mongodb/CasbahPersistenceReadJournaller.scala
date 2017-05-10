/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.actor.{ActorRef, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.mongodb.casbah.Imports._
import com.mongodb.{Bytes, DBObject}

import scala.concurrent.Future
import scala.util.Random

object CurrentAllPersistenceIds {
  def props(driver: CasbahMongoDriver): Props = Props(new CurrentAllPersistenceIds(driver))
}

class CurrentAllPersistenceIds(val driver: CasbahMongoDriver) extends SyncActorPublisher[String, Stream[String]] {
  import driver.CasbahSerializers._

  val temporaryCollectionName = s"persistenceids-${System.currentTimeMillis()}-${Random.nextInt(1000)}"
  val temporaryCollection: MongoCollection = driver.collection(temporaryCollectionName)

  override protected def initialCursor: Stream[String] = {
    driver.getJournalCollections().toStream
      .flatMap { journal =>
        journal.aggregate(
          MongoDBObject("$project" -> MongoDBObject(PROCESSOR_ID -> 1)) ::
            MongoDBObject("$group" -> MongoDBObject("_id" -> s"$$$PROCESSOR_ID")) ::
            MongoDBObject("$out" -> temporaryCollectionName) ::
            Nil).results
        temporaryCollection.find().toStream
      }
      .flatMap(_.getAs[String]("_id"))
  }

  override protected def next(c: Stream[String], atMost: Long): (Vector[String], Stream[String]) = {
    val (buf, remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[String]): Boolean = {
    c.isEmpty
  }

  override protected def discard(c: Stream[String]): Unit = {
    temporaryCollection.drop()
  }
}

object CurrentAllEvents {
  def props(offset: Long, driver: CasbahMongoDriver): Props = Props(new CurrentAllEvents(offset, driver))
}

class CurrentAllEvents(val offset: Long, val driver: CasbahMongoDriver) extends SyncActorPublisher[Event, Stream[Event]] {
  import driver.CasbahSerializers._

  override protected def initialCursor: Stream[Event] = {

    val query = MongoDBObject(TIMESTAMP->MongoDBObject("$gte"->offset))

    for {
      atom <- driver.getJournalCollections().map(_.find(query).toStream).fold(Stream.empty)(_ ++ _)
      event <- atom.getAs[MongoDBList](EVENTS).map{ xs =>
        xs.collect {
          case x: DBObject => x
        }
      }.getOrElse(Stream.empty)
    } yield {
      driver.deserializeJournal(event, atom.as[Long](TIMESTAMP))
    }
  }

  override protected def next(c: Stream[Event], atMost: Long): (Vector[Event], Stream[Event]) = {
    val (buf, remainder) = c.splitAt(atMost.toIntWithoutWrapping)
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
  import driver.CasbahSerializers._

  override protected def initialCursor: Stream[Event] = {

    for {
      atom <- driver.getJournal(persistenceId)
        .find((PROCESSOR_ID $eq persistenceId) ++ (FROM $lte toSeq) ++ (TO $gte fromSeq))
        .sort(MongoDBObject(TO -> 1))
        .toStream
      event <- atom.getAs[MongoDBList](EVENTS).map{ xs =>
        xs.collect {
          case x: DBObject => x
        }
      }.getOrElse(Stream.empty)
      if event.getAs[Long](SEQUENCE_NUMBER).exists(sn => sn >= fromSeq && sn <= toSeq)
    } yield {
      driver.deserializeJournal(event, atom.as[Long](TIMESTAMP))
    }
  }

  override protected def next(c: Stream[Event], atMost: Long): (Vector[Event], Stream[Event]) = {
    val (buf, remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[Event]): Boolean = c.isEmpty

  override protected def discard(c: Stream[Event]): Unit = ()
}

class CasbahMongoJournalStream(val driver: CasbahMongoDriver) extends JournalStream[MongoCollection] {
  import driver.CasbahSerializers._

  override def cursor(): MongoCollection = {
    val c = driver.realtime
    c.addOption(Bytes.QUERYOPTION_TAILABLE)
    c.addOption(Bytes.QUERYOPTION_AWAITDATA)
    c
  }

  override def publishEvents(): Unit = {
    implicit val ec = driver.querySideDispatcher
    Future {
      cursor().foreach { next =>
        if (next.keySet().contains(EVENTS)) {
          val events = next.as[MongoDBList](EVENTS).collect { case x: DBObject =>
            driver.deserializeJournal(x, next.as[Long](TIMESTAMP))
          }
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

  override def currentAllEvents(offset: Long)(implicit m: Materializer): Source[Event, NotUsed] =
    Source.actorPublisher[Event](CurrentAllEvents.props(offset, driver)).mapMaterializedValue(_ => NotUsed)

  override def currentPersistenceIds(implicit m: Materializer): Source[String, NotUsed] =
    Source.actorPublisher[String](CurrentAllPersistenceIds.props(driver)).mapMaterializedValue(_ => NotUsed)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed] =
    Source.actorPublisher[Event](CurrentEventsByPersistenceId.props(driver, persistenceId, fromSeq, toSeq)).mapMaterializedValue(_ => NotUsed)

  override def subscribeJournalEvents(subscriber: ActorRef): Unit = {
    driver.actorSystem.eventStream.subscribe(subscriber, classOf[Event])
    ()
  }
}
