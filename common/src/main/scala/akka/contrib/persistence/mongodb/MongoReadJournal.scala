package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.persistence.query._
import akka.persistence.query.javadsl.{CurrentEventsByPersistenceIdQuery => JCEBP, CurrentEventsByTagQuery => JCEBT, CurrentPersistenceIdsQuery => JCP, EventsByPersistenceIdQuery => JEBP, EventsByTagQuery => JEBT, PersistenceIdsQuery => JAPIQ}
import akka.persistence.query.scaladsl._
import akka.stream.javadsl.{Source => JSource}
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.stream.{javadsl => _, scaladsl => _, _}
import com.typesafe.config.Config

import scala.collection.mutable

object MongoReadJournal {
  val Identifier = "akka-contrib-mongodb-persistence-readjournal"
}

class MongoReadJournal(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {


  private[this] val impl = MongoPersistenceExtension(system)(config).readJournal
  private[this] implicit val materializer: Materializer = ActorMaterializer()(system)

  override def scaladslReadJournal(): scaladsl.ReadJournal = new ScalaDslMongoReadJournal(impl)

  override def javadslReadJournal(): javadsl.ReadJournal = new JavaDslMongoReadJournal(new ScalaDslMongoReadJournal(impl))
}

object ScalaDslMongoReadJournal {

  val eventToEventEnvelope: Flow[Event, EventEnvelope, NotUsed] =
    Flow[Event].zipWithIndex.map{ case (event, offset) => event.toEnvelope(Offset.sequence(offset)) }

  val eventPlusOffsetToEventEnvelope: Flow[(Event, Offset), EventEnvelope, NotUsed] =
    Flow[(Event,Offset)].map{ case(event, offset) => event.toEnvelope(offset) }

  implicit class RichFlow[Mat](source: Source[Event, Mat]) {
    def toEventEnvelopes: Source[EventEnvelope, Mat] =
      source.via(eventToEventEnvelope)
  }

  implicit class RichFlowWithOffsets[Mat](source: Source[(Event, Offset), Mat]) {
    def toEventEnvelopes: Source[EventEnvelope, Mat] =
      source.via(eventPlusOffsetToEventEnvelope)
  }
}

class ScalaDslMongoReadJournal(impl: MongoPersistenceReadJournallingApi)(implicit m: Materializer)
  extends scaladsl.ReadJournal
    with CurrentPersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with PersistenceIdsQuery
    with EventsByPersistenceIdQuery
    with EventsByTagQuery {

  import ScalaDslMongoReadJournal._

  def currentAllEvents(): Source[EventEnvelope, NotUsed] = impl.currentAllEvents.toEventEnvelopes

  override def currentPersistenceIds(): Source[String, NotUsed] = impl.currentPersistenceIds

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    impl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).toEventEnvelopes
  }

  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    require(tag != null, "Tag must not be null")
    require(impl.checkOffsetIsSupported(offset), s"Offset $offset is not supported by read journal")
    impl.currentEventsByTag(tag, offset).toEventEnvelopes
  }

  def allEvents(): Source[EventEnvelope, NotUsed] = {
    val pastSource = impl.currentAllEvents
    val realtimeSource = impl.liveEvents
//      Source.actorRef[(Event, Offset)](streamBufferSizeMaxConfig.getInt("all-events"), OverflowStrategy.dropTail)
//            .mapMaterializedValue(impl.subscribeJournalEvents)
//            .map{ case(e,_) => e }
    (pastSource ++ realtimeSource).via(new RemoveDuplicatedEventsByPersistenceId).toEventEnvelopes
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    val pastSource =
      impl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
        .withAttributes(
          Attributes.logLevels(Logging.InfoLevel, Logging.InfoLevel).and(Attributes.name("events-by-pid-current"))
        )

    val realtimeSource = impl
      .liveEventsByPersistenceId(persistenceId)
      .withAttributes(
        Attributes.logLevels(Logging.InfoLevel, Logging.InfoLevel).and(Attributes.name("events-by-pid-realtime"))
      )

    val stages = Flow[Event]
      .filter(_.pid == persistenceId)
      .filter(_.sn >= fromSequenceNr)
      .via(new StopAtSeq(toSequenceNr))
      .via(new RemoveDuplicatedEventsByPersistenceId)

    val liveSource = pastSource.concat(realtimeSource)

    liveSource
      .via(stages).toEventEnvelopes
  }

  override def persistenceIds(): Source[String, NotUsed] = {

    val pastSource = impl.currentPersistenceIds
    val realtimeSource = impl.livePersistenceIds
    (pastSource ++ realtimeSource).via(new RemoveDuplicates)
  }

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    require(tag != null, "Tag must not be null")
    require(impl.checkOffsetIsSupported(offset), s"Offset $offset is not supported by read journal")
    implicit val ordering: Ordering[Offset] = implicitly[Ordering[Offset]]
    val pastSource =
      impl.currentEventsByTag(tag, offset)
        .toEventEnvelopes
    val realtimeSource =
      impl.liveEventsByTag(tag, offset)
        .toEventEnvelopes
    (pastSource ++ realtimeSource).via(new RemoveDuplicatedEventEnvelopes)
  }
}

class JavaDslMongoReadJournal(rj: ScalaDslMongoReadJournal) extends javadsl.ReadJournal with JCP with JCEBP with JEBP with JAPIQ with JCEBT with JEBT {
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

  override def currentEventsByTag(tag: String, offset: Offset): JSource[EventEnvelope, NotUsed] = rj.currentEventsByTag(tag, offset).asJava

  override def eventsByTag(tag: String, offset: Offset): JSource[EventEnvelope, NotUsed] = rj.eventsByTag(tag, offset).asJava
}


trait JournalStream[Cursor] {
  protected val killSwitch: SharedKillSwitch = KillSwitches.shared("realtimeKillSwitch")

  def stopAllStreams(): Unit = killSwitch.shutdown()
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

// TODO: can cause lost events if upstream is out of sequence
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
        case Some(_) =>
          pull(in)
      }
    }
    override def onPull(): Unit = pull(in)

    setHandlers(in, out, this)
  }

}

// TODO: can cause lost events if upstream is out of sequence
class RemoveDuplicatedEventEnvelopes extends GraphStage[FlowShape[EventEnvelope, EventEnvelope]] {
  private val in: Inlet[EventEnvelope] = Inlet("in")
  private val out: Outlet[EventEnvelope] = Outlet("out")

  override val shape: FlowShape[EventEnvelope, EventEnvelope] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private val lastSequenceNrByPersistenceId = mutable.HashMap.empty[String, Long]

    override def onPush(): Unit = {
      val event = grab(in)
      lastSequenceNrByPersistenceId.get(event.persistenceId) match {
        case Some(sn) if event.sequenceNr > sn =>
          push(out, event)
          lastSequenceNrByPersistenceId.update(event.persistenceId, event.sequenceNr)
        case None =>
          push(out, event)
          lastSequenceNrByPersistenceId.update(event.persistenceId, event.sequenceNr)
        case Some(_) =>
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

  def currentEventsByTag(tag: String, offset: Offset)(implicit m: Materializer): Source[(Event, Offset), NotUsed]

  def checkOffsetIsSupported(offset: Offset): Boolean

  def liveEvents(implicit m: Materializer): Source[Event, NotUsed]

  def livePersistenceIds(implicit m: Materializer): Source[String, NotUsed]

  def liveEventsByPersistenceId(persistenceId: String)(implicit m: Materializer): Source[Event, NotUsed]

  def liveEventsByTag(tag: String, offset: Offset)(implicit m: Materializer, ord: Ordering[Offset]): Source[(Event, Offset), NotUsed]
}

trait SyncActorPublisher[A, Cursor] extends GraphStage[SourceShape[A]] {

  private val BUFSZ = 100L // TODO: Make configurable?
  private val out = Outlet[A]("out")
  override val shape: SourceShape[A] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private var cursor = initialCursor
      private var buffered = Vector.empty[A]

      override def onPull(): Unit = {
        if (buffered.isEmpty) {
          if (isCompleted(cursor)) {
            completeStage()
            discard(cursor)
          }
          else {
            val (batch, nextCursor) = next(cursor, BUFSZ)
            cursor = nextCursor
            buffered = batch
          }
        }
        buffered.headOption.foreach(push(out, _))
        buffered =
          if (buffered.nonEmpty) buffered.tail
          else buffered
      }

      override def onDownstreamFinish(): Unit = {
        discard(cursor)
      }

      setHandler(out, this)
    }

  protected def initialCursor: Cursor

  protected def next(c: Cursor, atMost: Long): (Vector[A], Cursor)

  protected def isCompleted(c: Cursor): Boolean

  protected def discard(c: Cursor): Unit
}
