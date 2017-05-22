package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Props, Status}
import akka.event.Logging
import akka.persistence.query._
import akka.persistence.query.javadsl.{PersistenceIdsQuery => JAPIQ, CurrentEventsByPersistenceIdQuery => JCEBP, CurrentPersistenceIdsQuery => JCP, EventsByPersistenceIdQuery => JEBP}
import akka.persistence.query.scaladsl._
import akka.stream.actor._
import akka.stream.javadsl.{Source => JSource}
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.stream.{javadsl => _, scaladsl => _, _}
import com.typesafe.config.Config

import scala.annotation.tailrec
import scala.collection.mutable

object MongoReadJournal {
  val Identifier = "akka-contrib-mongodb-persistence-readjournal"
}

class MongoReadJournal(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  private[this] val impl = MongoPersistenceExtension(system)(config).readJournal
  private[this] implicit val materializer = ActorMaterializer()(system)

  override def scaladslReadJournal(): scaladsl.ReadJournal = new ScalaDslMongoReadJournal(impl)

  override def javadslReadJournal(): javadsl.ReadJournal = new JavaDslMongoReadJournal(new ScalaDslMongoReadJournal(impl))
}

object ScalaDslMongoReadJournal {

  val eventToEventEnvelope: Flow[Event, EventEnvelope, NotUsed] = {
    // TODO Use zipWithIndex in akka 2.4.14
    Flow[Event].zip(Source.unfold(0L)(s => Some((s + 1, s)))).map { case (event, offset) => event.toEnvelope(Offset.sequence(offset)) }
  }

  implicit class RichFlow[Mat](source: Source[Event, Mat]) {

    def toEventEnvelopes: Source[EventEnvelope, Mat] =
      source.via(eventToEventEnvelope)
  }
}

class ScalaDslMongoReadJournal(impl: MongoPersistenceReadJournallingApi)(implicit m: Materializer)
  extends scaladsl.ReadJournal
    with CurrentPersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with PersistenceIdsQuery
    with EventsByPersistenceIdQuery {

  import ScalaDslMongoReadJournal._

  def currentAllEvents(): Source[EventEnvelope, NotUsed] = impl.currentAllEvents.toEventEnvelopes

  override def currentPersistenceIds(): Source[String, NotUsed] = impl.currentPersistenceIds

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    impl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).toEventEnvelopes
  }

  def allEvents(): Source[EventEnvelope, NotUsed] = {
    val pastSource = impl.currentAllEvents
    val realtimeSource = Source.actorRef(100, OverflowStrategy.dropTail).mapMaterializedValue(impl.subscribeJournalEvents)
    (pastSource ++ realtimeSource).via(new RemoveDuplicatedEventsByPersistenceId).toEventEnvelopes
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    val pastSource =
      impl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
        .withAttributes(Attributes.logLevels(Logging.InfoLevel, Logging.InfoLevel))

    val realtimeSource =
      Source.actorRef[Event](100, OverflowStrategy.dropTail)
        .mapMaterializedValue{ar => impl.subscribeJournalEvents(ar); NotUsed}
        .filter(_.pid == persistenceId)
        .filter(_.sn >= fromSequenceNr)
        .withAttributes(Attributes.logLevels(Logging.InfoLevel, Logging.InfoLevel))

    val liveSource = Source.actorPublisher[Event](
      Props(new LiveEventsByPersistenceId(pastSource, realtimeSource, persistenceId, fromSequenceNr, toSequenceNr))
    )

    val stages = Flow[Event]
      .filter(_.pid == persistenceId)
      .filter(_.sn >= fromSequenceNr)
      .via(new StopAtSeq(toSequenceNr))
      .via(new RemoveDuplicatedEventsByPersistenceId)

    liveSource
      .mapMaterializedValue(_ => NotUsed)
      .via(stages).toEventEnvelopes
  }

  override def persistenceIds(): Source[String, NotUsed] = {

    val pastSource = impl.currentPersistenceIds
    val realtimeSource = Source.actorRef[Event](100, OverflowStrategy.dropHead)
      .map(_.pid)
      .mapMaterializedValue{actor => impl.subscribeJournalEvents(actor); NotUsed}
    (pastSource ++ realtimeSource).via(new RemoveDuplicates)
  }
}

class JavaDslMongoReadJournal(rj: ScalaDslMongoReadJournal) extends javadsl.ReadJournal with JCP with JCEBP with JEBP with JAPIQ {
  def currentAllEvents(): JSource[EventEnvelope, NotUsed] = rj.currentAllEvents().asJava

  def allEvents(): JSource[EventEnvelope, NotUsed] = rj.allEvents().asJava

  override def currentPersistenceIds(): JSource[String, NotUsed] = rj.currentPersistenceIds().asJava

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): JSource[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    rj.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): JSource[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    rj.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
  }

  override def persistenceIds(): JSource[String, NotUsed] = rj.persistenceIds().asJava
}


trait JournalStream[Cursor] {
  def cursor(): Cursor

  def publishEvents(): Unit
}

private[mongodb] class LiveEventsByPersistenceId(pastSource: Source[Event,NotUsed],
                                                  realtimeSource: Source[Event,NotUsed],
                                                  persistenceId: String, minSequence: Long, maxSequence: Long)(implicit m: Materializer)
  extends ActorPublisher[Event] with ActorLogging {

  case object OnInit
  case object Ack
  case object Complete

  override def preStart(): Unit = {
    runStream(pastSource, minSequence)
  }

  override def receive: Receive = past(-1L, Nil)

  private def trySend(nextSn: Long, currentBuffer: Seq[Event], strictContiguous: Boolean): (Long,Seq[Event]) = {

    @tailrec
    def sendWhileRequested(atMost: Long, next: Long, buffer: Seq[Event]): (Long,Seq[Event]) = {
      if (atMost == 0) next -> buffer
      else {
        buffer.sortBy(_.sn).headOption match {
          case None => next -> buffer
          case Some(e) =>
            onNext(e)
            sendWhileRequested(atMost - 1, e.sn + 1, buffer.filterNot(_.sn == next))
        }
      }
    }

    @tailrec
    def sendWhileContiguous(atMost: Long, next: Long, buffer: Seq[Event]): (Long,Seq[Event]) = {
      if (atMost == 0) next -> buffer
      else {
        buffer.find(_.sn == next) match {
          case None => next -> buffer
          case Some(e) =>
            onNext(e)
            sendWhileContiguous(atMost - 1, next + 1, buffer.filterNot(_.sn == next))
        }
      }
    }

    val demand = totalDemand
    if (strictContiguous) sendWhileContiguous(demand, nextSn, currentBuffer)
    else sendWhileRequested(demand, nextSn, currentBuffer)
  }

  private def handleShutdownPublisherMessages: Receive = {
    case ActorPublisherMessage.Cancel =>
      log.debug("Downstream cancelled publisher")
      context.stop(self)
    case ActorPublisherMessage.SubscriptionTimeoutExceeded =>
      log.debug("Subscription timeout was exceeded")
      context.stop(self)
  }

  private def handleBasicActorRefSinkMessages(logHeader: String): Receive = {
    case OnInit =>
      log.debug(s"[$logHeader] Stream initialized")
      sender() ! Ack
    case Status.Failure(t) =>
      sender() ! Ack
      log.error(t,s"[$logHeader] Failure while streaming eventsByPersistenceId for id $persistenceId, stopping stream")
      context.stop(self)
  }

  private def past(nextSequenceNr: Long, buffered: Seq[Event]): Receive =
    handleShutdownPublisherMessages orElse
      handleBasicActorRefSinkMessages("past") orElse {
        case ActorPublisherMessage.Request(_) =>
          if (nextSequenceNr > -1L) {
            val (next,buf) = trySend(nextSequenceNr, buffered, strictContiguous = false)
            context.become(past(next, buf))
          }
        case e:Event =>
          val startingProblemSn = if (nextSequenceNr == -1L) e.sn else nextSequenceNr
          sender() ! Ack
          val (next,buf) = trySend(startingProblemSn, buffered :+ e, strictContiguous = false)
          context.become(past(next, buf))
        case Complete =>
          sender() ! Ack
          runStream(realtimeSource,nextSequenceNr)
          log.debug(s"Past completed for $persistenceId, transitioning to live @$nextSequenceNr with buffer of size ${buffered.size}")
          context.become(live(nextSequenceNr, buffered)) // transition to realtime, maintaining last sn
      }

  private def live(nextSequenceNr: Long, buffered: Seq[Event]): Receive =
    handleShutdownPublisherMessages orElse
      handleBasicActorRefSinkMessages("current") orElse {
      case ActorPublisherMessage.Request(_) =>
        val (next,buf) = trySend(nextSequenceNr, buffered, strictContiguous = true)
        context.become(live(next, buf))
      case e:Event =>
        val startingProblemSn = if (nextSequenceNr == -1L) e.sn else nextSequenceNr
        sender() ! Ack
        val (next,buf) = trySend(startingProblemSn, buffered :+ e, strictContiguous = true)
        context.become(live(next, buf))
      case Complete =>
        log.debug(s"Upstream completed - stopping publisher for persistenceId $persistenceId")
        sender() ! Ack
        context.stop(self)
    }

  private def runStream(source: Source[Event,NotUsed], minSn: Long) = {
    source
      .filter(_.pid == persistenceId)
      .filter(_.sn >= minSn)
      .runWith(Sink.actorRefWithAck(self, OnInit, Ack, Complete, Status.Failure))
    ()
  }
}

class StopAtSeq(to: Long) extends GraphStage[FlowShape[Event, Event]] {
  val in: Inlet[Event] = Inlet[Event]("flowIn")
  val out: Outlet[Event] = Outlet[Event]("flowOut")

  override def shape: FlowShape[Event, Event] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val ev = grab(in)
        push(out, ev)
        if (ev.sn == to) completeStage()
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })
  }
}

class RemoveDuplicatedEventsByPersistenceId extends GraphStage[FlowShape[Event, Event]] {

  private val in: Inlet[Event] = Inlet("in")
  private val out: Outlet[Event] = Outlet("out")

  override val shape: FlowShape[Event, Event] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private val lastSequenceNrByPersistenceId = mutable.HashMap.empty[String, Long]

    override def onPush(): Unit = {
      val event = grab(in)
      lastSequenceNrByPersistenceId.get(event.pid) match {
        case Some(sn) if event.sn > sn =>
          push(out, event)
          lastSequenceNrByPersistenceId.update(event.pid, event.sn)
        case None =>
          push(out, event)
          lastSequenceNrByPersistenceId.update(event.pid, event.sn)
        case Some(sn) =>
          pull(in)
      }
    }
    override def onPull(): Unit = pull(in)

    setHandlers(in, out, this)
  }

}

class RemoveDuplicates[T] extends GraphStage[FlowShape[T, T]] {

  private val in: Inlet[T] = Inlet("in")
  private val out: Outlet[T] = Outlet("out")

  override val shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private var processed = Set.empty[T]

    override def onPush(): Unit = {
      val element = grab(in)
      if(processed.contains(element)) {
        pull(in)
      } else {
        processed += element
        push(out, element)
      }
    }

    override def onPull(): Unit = pull(in)

    setHandlers(in, out, this)
  }

}

trait MongoPersistenceReadJournallingApi {
  def currentAllEvents(implicit m: Materializer): Source[Event, NotUsed]

  def currentPersistenceIds(implicit m: Materializer): Source[String, NotUsed]

  def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed]

  def subscribeJournalEvents(subscriber: ActorRef): Unit
}

trait SyncActorPublisher[A, Cursor] extends ActorPublisher[A] with ActorLogging {

  import ActorPublisherMessage._

  override def preStart(): Unit = {
    context.become(streaming(initialCursor, 0))
    super.preStart()
  }

  protected def driver: MongoPersistenceDriver

  protected def initialCursor: Cursor

  protected def next(c: Cursor, atMost: Long): (Vector[A], Cursor)

  protected def isCompleted(c: Cursor): Boolean

  protected def discard(c: Cursor): Unit

  def receive: Receive = Actor.emptyBehavior

  def streaming(cursor: Cursor, offset: Long): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded =>
      discard(cursor)
      context.stop(self)
    case Request(_) =>
      val (filled, remaining) = next(cursor, totalDemand)
      filled foreach onNext
      if (isCompleted(remaining)) {
        onCompleteThenStop()
        discard(remaining)
      }
      else
        context.become(streaming(remaining, offset + filled.size))
  }
}
