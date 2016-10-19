/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor._
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.stream.actor.ActorPublisher
import akka.{ Done => ADone }
import play.api.libs.iteratee._
import reactivemongo.api.QueryOpts
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson._

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.play.iteratees.cursorProducer

trait IterateeActorPublisher[T] extends ActorPublisher[T] with ActorLogging {

  import akka.pattern.pipe
  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  def initial: Enumerator[T]

  override def preStart() = {
    context.become(awaiting(initial andThen Enumerator.eof[T]))
  }

  override def receive: Receive = Actor.emptyBehavior

  private case class Next(enumerator: Enumerator[T], iteratee: Iteratee[T, Unit])

  private def respondToDemand(enumerator: Enumerator[T], iter: Iteratee[T, Unit]) = {
    Concurrent.runPartial(enumerator, iter).map { case (_, e) => Next(e, iter) }.pipeTo(self)
  }

  private class DoneIteratee extends Iteratee[T, Unit] {
    override def fold[B](folder: (Step[T, Unit]) => Future[B])(implicit ec: ExecutionContext): Future[B] = {
      folder(Step.Done((), Input.Empty))
    }
  }

  private class CursorIteratee extends Iteratee[T, Unit] {
    override def fold[B](folder: (Step[T, Unit]) => Future[B])(implicit ec: ExecutionContext): Future[B] = {
      folder(Step.Cont({
        case Input.El(elem) =>
          onNext(elem)
          if (totalDemand > 0L) this
          else new DoneIteratee
        case Input.EOF =>
          onComplete()
          cleanup().pipeTo(self)
          new DoneIteratee
        case Input.Empty =>
          if (totalDemand > 0L) this
          else new DoneIteratee
      }))
    }
  }

  def cleanup(): Future[ADone] = Future.successful(ADone)

  def defaults: Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded =>
      log.warning("Cancelling stream")
      onCompleteThenStop()
      cleanup().pipeTo(self)
      ()
    case Status.Failure(t) =>
      log.error(t, "Failure occurred while streaming")
      onErrorThenStop(t)
      cleanup().pipeTo(self)
      ()
    case ADone =>
      context.stop(self)
  }

  def awaiting(enumerator: Enumerator[T]): Receive = defaults orElse handleNext("Awaiting") orElse {
    case Request(_) =>
      log.debug(s"Awaiting: Request received, demand = $totalDemand")
      respondToDemand(enumerator, new CursorIteratee)
      context.become(streaming(enumerator))
  }

  def streaming(enumerator: Enumerator[T]): Receive = defaults orElse handleNext("Streaming")

  private def handleNext(header: String): Receive = {
    case Next(enm, it) =>
      log.debug("Next received")
      if (totalDemand > 0) {
        log.debug(s"$header: requesting more, demand = $totalDemand")
        respondToDemand(enm, it)
        context.become(streaming(enm))
      } else {
        log.debug(s"$header: nothing to do, no demand")
        context.become(awaiting(enm))
      }
  }

}

object CurrentAllEvents {
  def props(driver: RxMongoDriver) = Props(new CurrentAllEvents(driver))
}

class CurrentAllEvents(val driver: RxMongoDriver) extends IterateeActorPublisher[Event] {
  import JournallingFieldNames._
  import RxMongoSerializers._
  import context.dispatcher

  private val flatten: Enumeratee[BSONDocument, Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d: BSONDocument => driver.deserializeJournal(d)
      }: _*)
  }

  private val flattenCollection: Enumeratee[BSONCollection, BSONDocument] = Enumeratee.mapFlatten[BSONCollection] { journal =>
    journal.find(BSONDocument())
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .enumerator()
      .map(doc => doc) // this is needed for suffix collections
  }

  override def initial: Enumerator[Event] = {

    driver.getJournalCollections()
      .through(flattenCollection)
      .through(flatten)
  }
}

object CurrentAllPersistenceIds {
  def props(driver: RxMongoDriver) = Props(new CurrentAllPersistenceIds(driver))
}

class CurrentAllPersistenceIds(val driver: RxMongoDriver) extends IterateeActorPublisher[String] {
  import JournallingFieldNames._
  import context.dispatcher
  import reactivemongo.bson._

  private val flattenCollection: Enumeratee[BSONCollection, String] = Enumeratee.mapFlatten[BSONCollection] { journal =>

    var retrievedPids: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty

    journal.find(BSONDocument())
      .projection(BSONDocument(PROCESSOR_ID -> 1))
      .cursor[BSONDocument]()
      .enumerator()
      .map(_.getAs[String](PROCESSOR_ID) match {
        case Some(pid) if (pid != null && !pid.trim.isEmpty && !retrievedPids.contains(pid.trim)) =>
          retrievedPids += pid.trim
          pid
      })

  }

  override def initial: Enumerator[String] = {
    driver.getJournalCollections()
      .through(flattenCollection)
  }
}

object CurrentEventsByPersistenceId {
  def props(driver: RxMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long): Props =
    Props(new CurrentEventsByPersistenceId(driver, persistenceId, fromSeq, toSeq))
}

class CurrentEventsByPersistenceId(val driver: RxMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long) extends IterateeActorPublisher[Event] {
  import JournallingFieldNames._
  import RxMongoSerializers._
  import context.dispatcher

  private val flatten: Enumeratee[BSONDocument, Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d: BSONDocument => driver.deserializeJournal(d)
      }: _*)
  }

  private val filter = Enumeratee.filter[Event] { e =>
    e.sn >= fromSeq && e.sn <= toSeq
  }

  override def initial = Enumerator.flatten {

    val q = BSONDocument(
      PROCESSOR_ID -> persistenceId,
      TO -> BSONDocument("$gte" -> fromSeq),
      FROM -> BSONDocument("$lte" -> toSeq))

    driver.getJournal(persistenceId)
      .map(_.find(q)
        .sort(BSONDocument(TO -> 1))
        .projection(BSONDocument(EVENTS -> 1))
        .cursor[BSONDocument]()
        .enumerator()
        .through(flatten)
        .through(filter))
  }
}

class RxMongoJournalStream(driver: RxMongoDriver) extends JournalStream[Enumerator[BSONDocument]] {
  import RxMongoSerializers._

  implicit val ec = driver.querySideDispatcher

  override def cursor() =
    Enumerator.flatten(
      driver.realtime.map(rt =>
        rt.find(BSONDocument.empty)
          .options(QueryOpts().tailable.awaitData)
          .cursor[BSONDocument]()
          .enumerator()
          ))

  private val flatten: Enumeratee[BSONDocument, Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d: BSONDocument => driver.deserializeJournal(d)
      }: _*)
  }

  override def publishEvents() = {
    val iteratee = Iteratee.foreach[Event](driver.actorSystem.eventStream.publish)
    cursor().through(flatten).run(iteratee)
    ()
  }
}

class RxMongoReadJournaller(driver: RxMongoDriver) extends MongoPersistenceReadJournallingApi {

  val journalStream = {
    val stream = new RxMongoJournalStream(driver)
    stream.publishEvents()
    stream
  }

  override def currentAllEvents: Props = CurrentAllEvents.props(driver)

  override def currentPersistenceIds: Props = CurrentAllPersistenceIds.props(driver)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long): Props =
    CurrentEventsByPersistenceId.props(driver, persistenceId, fromSeq, toSeq)

  override def subscribeJournalEvents(subscriber: ActorRef): Unit = {
    driver.actorSystem.eventStream.subscribe(subscriber, classOf[Event])
    ()
  }
}
