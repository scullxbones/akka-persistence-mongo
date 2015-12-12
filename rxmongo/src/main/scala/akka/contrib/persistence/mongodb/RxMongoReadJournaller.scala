package akka.contrib.persistence.mongodb

import akka.actor._
import akka.contrib.persistence.mongodb.Event
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.persistence.query.EventEnvelope
import akka.stream.actor.ActorPublisher
import play.api.libs.iteratee.{Concurrent, Enumeratee, Iteratee, Enumerator}
import reactivemongo.api.commands.Command
import reactivemongo.api.{BSONSerializationPack, QueryOpts}
import reactivemongo.bson._

trait IterateeActorPublisher[T] extends ActorPublisher[T] with Stash {

  import akka.pattern.pipe
  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  def initial: Enumerator[T]

  override def preStart() = {
    context.become(streaming(initial andThen Enumerator.eof[T] through Enumeratee.onEOF(onCompleteThenStop)))
  }

  override def receive: Receive = Actor.emptyBehavior

  case class Continue(enm: Enumerator[T])
  case class Failure(t: Throwable)

  def streaming(enumerator: Enumerator[T]): Receive = {
    case _:Cancel|SubscriptionTimeoutExceeded =>
      onCompleteThenStop()
    case Request(_) =>
      Concurrent.runPartial(enumerator,next).map {
        case (_,enm) => Continue(enm)
      }.pipeTo(self)
      context.become(publishing)
  }

  def publishing: Receive = {
    case Continue(enm) =>
      context.become(streaming(enm))
      unstashAll()
    case Status.Failure(t) =>
      onErrorThenStop(t)
    case x =>
      stash()
  }

  private val onNextIteratee = Iteratee.foreach[T](onNext)
  private def next = {
    Enumeratee take totalDemand.toIntWithoutWrapping transform onNextIteratee
  }

}

object CurrentAllEvents {
  def props(driver: RxMongoDriver) = Props(new CurrentAllEvents(driver))
}

class CurrentAllEvents(val driver: RxMongoDriver) extends IterateeActorPublisher[Event] {
  import RxMongoSerializers._
  import JournallingFieldNames._
  import context.dispatcher

  private val opts = QueryOpts().noCursorTimeout

  private val flatten: Enumeratee[BSONDocument,Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      } : _*
    )
  }

  override def initial: Enumerator[Event] = {
    driver.journal
      .find(BSONDocument())
      .options(opts)
      .sort(BSONDocument(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1))
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .enumerate()
      .through(flatten)
  }
}

object CurrentAllPersistenceIds {
  def props(driver: RxMongoDriver) = Props(new CurrentAllPersistenceIds(driver))
}

class CurrentAllPersistenceIds(val driver: RxMongoDriver) extends IterateeActorPublisher[String] {
  import JournallingFieldNames._
  import context.dispatcher

  private val flatten: Enumeratee[BSONDocument,String] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(doc.getAs[Vector[String]]("values").get : _*)
  }

  override def initial = {
    val q = BSONDocument("distinct" -> driver.journalCollectionName, "key" -> PROCESSOR_ID, "query" -> BSONDocument())
    val cmd = Command.run(BSONSerializationPack)
    cmd(driver.db,cmd.rawCommand(q))
      .cursor[BSONDocument]
      .enumerate()
      .through(flatten)
  }
}

object CurrentEventsByPersistenceId {
  def props(driver:RxMongoDriver,persistenceId:String,fromSeq:Long,toSeq:Long):Props =
    Props(new CurrentEventsByPersistenceId(driver,persistenceId,fromSeq,toSeq))
}

class CurrentEventsByPersistenceId(val driver:RxMongoDriver,persistenceId:String,fromSeq:Long,toSeq:Long) extends IterateeActorPublisher[Event] {
  import JournallingFieldNames._
  import RxMongoSerializers._
  import context.dispatcher

  private val flatten: Enumeratee[BSONDocument,Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      } : _*
    )
  }

  private val filter = Enumeratee.filter[Event] { e =>
    e.sn >= fromSeq && e.sn <= toSeq
  }

  override def initial = {
    val q = BSONDocument(
      PROCESSOR_ID -> persistenceId,
      FROM -> BSONDocument("$gte" -> fromSeq),
      FROM -> BSONDocument("$lte" -> toSeq)
    )
    driver.journal.find(q)
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .enumerate()
      .through(flatten)
      .through(filter)
  }
}

class RxMongoJournalStream(driver: RxMongoDriver) extends JournalStream[Enumerator[BSONDocument]]{
  import concurrent.ExecutionContext.Implicits.global
  import RxMongoSerializers._
  override def cursor = driver.realtime.find(BSONDocument.empty).options(QueryOpts().tailable.awaitData).cursor[BSONDocument]().enumerate()

  private val flatten: Enumeratee[BSONDocument,Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      } : _*
    )
  }


  override def publishEvents = {
    val iteratee = Iteratee.foreach[Event](driver.actorSystem.eventStream.publish)
    cursor.through(flatten).run(iteratee)
  }
}

class RxMongoReadJournaller(driver: RxMongoDriver) extends MongoPersistenceReadJournallingApi {

  val journalStream = new RxMongoJournalStream(driver)
  journalStream.publishEvents

  override def currentAllEvents: Props = CurrentAllEvents.props(driver)

  override def currentPersistenceIds: Props = CurrentAllPersistenceIds.props(driver)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long): Props =
    CurrentEventsByPersistenceId.props(driver,persistenceId,fromSeq,toSeq)

  override def subscribeJournalEvents(subscriber: ActorRef): Unit =
    driver.actorSystem.eventStream.subscribe(subscriber, classOf[Event])
}
