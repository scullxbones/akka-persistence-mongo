package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, Actor, ExtendedActorSystem, Props}
import akka.event.{LookupClassification, ActorEventBus}
import akka.persistence.query._
import akka.persistence.query.scaladsl.{AllPersistenceIdsQuery, EventsByPersistenceIdQuery, CurrentEventsByPersistenceIdQuery, CurrentPersistenceIdsQuery}
import akka.persistence.query.javadsl.{CurrentEventsByPersistenceIdQuery => JCEBP, CurrentPersistenceIdsQuery => JCP, EventsByPersistenceIdQuery => JEBP, AllPersistenceIdsQuery => JAPIQ}
import akka.stream.{SourceShape, OverflowStrategy}
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.scaladsl.{Flow, MergePreferred, FlowGraph, Source}
import akka.stream.javadsl.{Source => JSource}
import akka.stream.stage.{SyncDirective, Context, PushStage}
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

  def currentAllEvents(): Source[EventEnvelope,Unit] =
    Source.actorPublisher[Event](impl.currentAllEvents)
      .via(Flow[Event].transform(() => new EventEnvelopeConverter)).mapMaterializedValue(_ => ())

  override def currentPersistenceIds(): Source[String, Unit] =
    Source.actorPublisher[String](impl.currentPersistenceIds)
      .mapMaterializedValue(_ => ())

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, Unit] = {
    require(persistenceId != null, "PersistenceId must not be null")
    Source.actorPublisher[Event](impl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr))
      .via(Flow[Event].transform(() => new EventEnvelopeConverter)).mapMaterializedValue(_ => ())
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, Unit] = {
    require(persistenceId != null, "PersistenceId must not be null")
    val graph = FlowGraph.partial() { implicit builder =>
      import FlowGraph.Implicits._
      val merge = builder.add(MergePreferred[Event](1))

      val pastSource = builder.add(Source.actorPublisher[Event](impl.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr))
        .mapMaterializedValue(_ => ()))
      val realtimeSource = builder.add(Source.actorRef[Event](100, OverflowStrategy.dropHead)
        .mapMaterializedValue(actor => impl.subscribeJournalEvents(actor)))
      val filterByPersistenceId = builder.add(Flow[Event].filter(_.pid equals persistenceId))
      val removeDuplicatedEvents = builder.add(Flow[Event].transform(() => new RemoveDuplicatedEvents))
      val eventConverter = builder.add(Flow[Event].transform(() => new EventEnvelopeConverter))

      pastSource     ~>       merge.preferred
      realtimeSource ~>       merge.in(0)
                              merge.out  ~> filterByPersistenceId ~> removeDuplicatedEvents ~> eventConverter
      SourceShape(eventConverter.outlet)
    }
    Source.wrap(graph)
  }

  override def allPersistenceIds(): Source[String, Unit] = {
    val graph = FlowGraph.partial(){ implicit builder =>
      import FlowGraph.Implicits._
      val merge = builder.add(MergePreferred[String](1))

      val pastSource = builder.add(Source.actorPublisher[String](impl.currentPersistenceIds))
      val realtimeSource = builder.add(Source.actorRef[Event](100, OverflowStrategy.dropHead)
        .map(_.pid).mapMaterializedValue( actor => impl.subscribeJournalEvents(actor)))
      val removeDuplicatedpersistenceIds = builder.add(Flow[String].transform(() => new RemoveDuplicatedPersistenceId))
      pastSource     ~> merge.preferred
      realtimeSource ~> merge.in(0)
                        merge.out ~> removeDuplicatedpersistenceIds
      SourceShape(removeDuplicatedpersistenceIds.outlet)
    }
    Source.wrap(graph)
  }
}

class JavaDslMongoReadJournal(rj: ScalaDslMongoReadJournal) extends javadsl.ReadJournal with JCP with JCEBP with JEBP with JAPIQ{
  def currentAllEvents(): JSource[EventEnvelope, Unit] = rj.currentAllEvents().asJava

  override def currentPersistenceIds(): JSource[String, Unit] = rj.currentPersistenceIds().asJava

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): JSource[EventEnvelope, Unit] = {
    require(persistenceId != null, "PersistenceId must not be null")
    rj.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
  }
  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long) = {
    require(persistenceId != null, "PersistenceId must not be null")
    rj.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
  }

  override def allPersistenceIds(): JSource[String, Unit] = rj.allPersistenceIds().asJava
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
  var offset = -1
  override def onPush(elem: Event, ctx: Context[EventEnvelope]) = {
    offset += 1
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
