package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ExtendedActorSystem, Props}
import akka.persistence.query._
import akka.persistence.query.javadsl.{AllPersistenceIdsQuery => JAPIQ, CurrentEventsByPersistenceIdQuery => JCEBP, CurrentPersistenceIdsQuery => JCP, EventsByPersistenceIdQuery => JEBP}
import akka.persistence.query.scaladsl.{AllPersistenceIdsQuery, CurrentEventsByPersistenceIdQuery, CurrentPersistenceIdsQuery, EventsByPersistenceIdQuery}
import akka.stream.OverflowStrategy
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.javadsl.{Source => JSource}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage.{Context, PushStage, SyncDirective}
import com.typesafe.config.Config

import scala.collection.mutable

object MongoReadJournal {
  val Identifier = "akka-contrib-mongodb-persistence-readjournal"
}

class MongoReadJournal(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  private[this] val impl = MongoPersistenceExtension(system)(config).readJournal

  override def scaladslReadJournal(): scaladsl.ReadJournal = new ScalaDslMongoReadJournal(impl)

  override def javadslReadJournal(): javadsl.ReadJournal = new JavaDslMongoReadJournal(new ScalaDslMongoReadJournal(impl))
}

class ScalaDslMongoReadJournal(impl: MongoPersistenceReadJournallingApi)
    extends scaladsl.ReadJournal
    with CurrentPersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with AllPersistenceIdsQuery
    with EventsByPersistenceIdQuery{

  def currentAllEvents(): Source[EventEnvelope, NotUsed] =
    Source.actorPublisher[Event](impl.currentAllEvents)
      .via(Flow[Event].transform(() => new EventEnvelopeConverter)).mapMaterializedValue(_ => NotUsed)

  override def currentPersistenceIds(): Source[String, NotUsed] =
    Source.actorPublisher[String](impl.currentPersistenceIds)
      .mapMaterializedValue(_ => NotUsed)

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    Source.actorPublisher[Event](impl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr))
      .via(Flow[Event].transform(() => new EventEnvelopeConverter)).mapMaterializedValue(_ => NotUsed)
  }

  def allEvents(): Source[EventEnvelope, NotUsed] = {

    val pastSource = Source.actorPublisher[Event](impl.currentAllEvents).mapMaterializedValue(_ => ())
    val realtimeSource = Source.actorRef[Event](100, OverflowStrategy.dropHead)
      .mapMaterializedValue(actor => impl.subscribeJournalEvents(actor))
    val removeDuplicatedEventsByPersistenceId = Flow[Event].transform(() => new RemoveDuplicatedEventsByPersistenceId)
    val eventEnvelopeConverter = Flow[Event].transform(() => new EventEnvelopeConverter)
    (pastSource ++ realtimeSource).mapMaterializedValue(_ => NotUsed).via(removeDuplicatedEventsByPersistenceId).via(eventEnvelopeConverter)
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    val pastSource = Source.actorPublisher[Event](impl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr))
      .mapMaterializedValue(_ => ())
    val realtimeSource = Source.actorRef[Event](100, OverflowStrategy.dropHead)
      .mapMaterializedValue(actor => impl.subscribeJournalEvents(actor))
    val filterByPersistenceId = Flow[Event].filter(_.pid equals persistenceId)
    val removeDuplicatedEvents = Flow[Event].transform(() => new RemoveDuplicatedEvents)
    val eventConverter = Flow[Event].transform(() => new EventEnvelopeConverter)
    (pastSource ++ realtimeSource).mapMaterializedValue(_ => NotUsed).via(filterByPersistenceId).via(removeDuplicatedEvents).via(eventConverter)
  }

  override def allPersistenceIds(): Source[String, NotUsed] = {

      val pastSource = Source.actorPublisher[String](impl.currentPersistenceIds)
      val realtimeSource = Source.actorRef[Event](100, OverflowStrategy.dropHead)
        .map(_.pid).mapMaterializedValue( actor => impl.subscribeJournalEvents(actor))
      val removeDuplicatedpersistenceIds = Flow[String].transform(() => new RemoveDuplicatedPersistenceId)

    (pastSource ++ realtimeSource).mapMaterializedValue(_ => NotUsed).via(removeDuplicatedpersistenceIds)
  }
}

class JavaDslMongoReadJournal(rj: ScalaDslMongoReadJournal) extends javadsl.ReadJournal with JCP with JCEBP with JEBP with JAPIQ{
  def currentAllEvents(): JSource[EventEnvelope, NotUsed] = rj.currentAllEvents().asJava
  def allEvents(): JSource[EventEnvelope, NotUsed] = rj.allEvents().asJava

  override def currentPersistenceIds(): JSource[String, NotUsed] = rj.currentPersistenceIds().asJava

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): JSource[EventEnvelope, NotUsed] = {
    require(persistenceId != null, "PersistenceId must not be null")
    rj.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
  }
  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) = {
    require(persistenceId != null, "PersistenceId must not be null")
    rj.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
  }

  override def allPersistenceIds(): JSource[String, NotUsed] = rj.allPersistenceIds().asJava
}


trait JournalStream[Cursor] {
  def cursor(): Cursor
  def publishEvents(): Unit
}

class RemoveDuplicatedEvents extends PushStage[Event, Event]{
  var lastSequenceNr: Option[Long] = None
  override def onPush(elem: Event, ctx: Context[Event]) = {
    lastSequenceNr match {
      case Some(sequenceNr) =>
        if (elem.sn > sequenceNr) {
          lastSequenceNr = Some(elem.sn)
          ctx.push(elem)
        }else{
          ctx.pull()
        }
      case None =>
        lastSequenceNr = Some(elem.sn)
        ctx.push(elem)
    }
  }
}

class RemoveDuplicatedEventsByPersistenceId extends PushStage[Event, Event]{
  val lastSequenceNrByPersistenceId = mutable.HashMap.empty[String, Long]
  override def onPush(elem: Event, ctx: Context[Event]) = {
    lastSequenceNrByPersistenceId get elem.pid match {
      case Some(sequenceNr) =>
        if (elem.sn > sequenceNr) {
          lastSequenceNrByPersistenceId remove elem.pid
          lastSequenceNrByPersistenceId += (elem.pid -> elem.sn)
          ctx.push(elem)
        }else{
          ctx.pull()
        }
      case None =>
        lastSequenceNrByPersistenceId += (elem.pid -> elem.sn)
        ctx.push(elem)
    }
  }
}

class RemoveDuplicatedPersistenceId extends PushStage[String, String] {
  val persistenceIds = mutable.HashSet.empty[String]
  override def onPush(elem: String, ctx: Context[String]): SyncDirective = {
    if (persistenceIds(elem)) ctx.pull()
    else {
      persistenceIds += elem
      ctx push elem
    }
  }
}

class EventEnvelopeConverter extends PushStage[Event, EventEnvelope] {
  var offset = -1L
  override def onPush(elem: Event, ctx: Context[EventEnvelope]) = {
    offset += 1L
    ctx.push(elem.toEnvelope(offset))
  }
}

trait MongoPersistenceReadJournallingApi {
  def currentAllEvents: Props
  def currentPersistenceIds: Props
  def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long): Props
  def subscribeJournalEvents(subscriber: ActorRef): Unit
}

trait SyncActorPublisher[A,Cursor] extends ActorPublisher[A] {
  import ActorPublisherMessage._

  override def preStart() = {
    context.become(streaming(initialCursor, 0))
    super.preStart()
  }

  protected def driver: MongoPersistenceDriver

  protected def initialCursor: Cursor

  protected def next(c: Cursor, atMost: Long): (Vector[A], Cursor)

  protected def isCompleted(c: Cursor): Boolean

  protected def discard(c: Cursor): Unit

  def receive = Actor.emptyBehavior

  def streaming(cursor: Cursor, offset: Long): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded =>
      discard(cursor)
      context.stop(self)
    case Request(_) =>
      val (filled,remaining) = next(cursor, totalDemand)
      filled foreach onNext
      if (isCompleted(remaining))
        onCompleteThenStop()
      else
        context.become(streaming(remaining, offset + filled.size))
  }
}
