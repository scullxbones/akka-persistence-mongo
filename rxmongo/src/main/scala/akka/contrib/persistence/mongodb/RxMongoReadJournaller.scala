package akka.contrib.persistence.mongodb

import akka.actor._
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.stream.actor.ActorPublisher
import play.api.libs.iteratee._
import reactivemongo.api.commands.Command
import reactivemongo.api.{BSONSerializationPack, QueryOpts}
import reactivemongo.bson._

trait IterateeActorPublisher[T] extends ActorPublisher[T] with ActorLogging {

  import akka.pattern.pipe
  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  def initial: Enumerator[T]

  override def preStart() = {
    context.become(awaiting(initial andThen Enumerator.eof[T]))
  }

  override def receive: Receive = Actor.emptyBehavior

  private case class Next(enumerator: Enumerator[T], iteratee: Iteratee[T,Unit])

  private var completed: Boolean = false

  private def respondToDemand(enumerator: Enumerator[T], iter: Iteratee[T,Unit]) = {
    enumerator(iter).map(Next(enumerator,_)).pipeTo(self)
  }

  val nextElem: Iteratee[T,Unit] = {
    Cont {
      case Input.EOF =>
        onComplete()
        completed = true
        Done(())
      case Input.Empty =>
        nextElem
      case Input.El(elem) =>
        onNext(elem)
        nextElem
    }
  }

  def defaults: Receive = {
    case _:Cancel|SubscriptionTimeoutExceeded =>
      log.debug(s"Cancelling stream")
      onCompleteThenStop()
    case Status.Failure(t) =>
      log.error(t,"Failure occurred while streaming")
      onErrorThenStop(t)
  }

  def awaiting(enumerator: Enumerator[T]): Receive = defaults orElse handleNext("Awaiting") orElse {
    case Request(_) =>
      log.debug(s"Awaiting: Request received, demand = $totalDemand")
      respondToDemand(enumerator, nextElem)
      context.become(streaming(enumerator))
  }

  def streaming(enumerator: Enumerator[T]): Receive = defaults orElse handleNext("Streaming")

  private def handleNext(header: String): Receive = {
    case Next(enm, it) =>
      log.debug("Next received")
      if (completed) {
        log.debug(s"$header: completed - now stopping")
        context.stop(self)
      }
      else if (totalDemand > 0) {
        log.debug(s"$header: requesting more, demand = $totalDemand")
        respondToDemand(enm, it)
        context.become(streaming(enm))
      }
      else {
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
  import reactivemongo.bson._

  private def flatten(doc: BSONDocument): Enumerator[String] = {
    val result = for {
      arr <- doc.getAs[BSONArray]("values").toStream
      elem <- arr.values
      bson <- elem.seeAsOpt[BSONString].toStream
    } yield bson.value
    Enumerator.enumerate[String](result)
  }

  override def initial = {
    val q = BSONDocument("distinct" -> driver.journalCollectionName, "key" -> PROCESSOR_ID, "query" -> BSONDocument())
    val cmd = Command.run(BSONSerializationPack)
    cmd(driver.db,cmd.rawCommand(q))
      .cursor[BSONDocument]
      .enumerate()
      .flatMap(flatten)
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
  import RxMongoSerializers._

  implicit val ec = driver.querySideDispatcher

  override def cursor() = driver.realtime.find(BSONDocument.empty).options(QueryOpts().tailable.awaitData).cursor[BSONDocument]().enumerate()

  private val flatten: Enumeratee[BSONDocument,Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      } : _*
    )
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
    CurrentEventsByPersistenceId.props(driver,persistenceId,fromSeq,toSeq)

  override def subscribeJournalEvents(subscriber: ActorRef): Unit = {
    driver.actorSystem.eventStream.subscribe(subscriber, classOf[Event])
    ()
  }
}
