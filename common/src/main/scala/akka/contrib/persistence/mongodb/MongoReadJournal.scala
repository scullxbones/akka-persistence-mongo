package akka.contrib.persistence.mongodb

import akka.actor.{Actor, ExtendedActorSystem, Props}
import akka.contrib.persistence.mongodb.MongoReadJournal.AllEvents
import akka.persistence.query._
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.scaladsl.Source

object MongoReadJournal {
  val Identifier = "akka-contrib-mongodb-persistence-readjournal"

  case object AllEvents extends Query[EventEnvelope, Unit]
}

class MongoReadJournal(system: ExtendedActorSystem) extends ReadJournal {

  private[this] val impl = MongoPersistenceExtension(system).readJournal

  override def query[T, M](q: Query[T, M], hints: Hint*): Source[T, M] = q match {
    case AllPersistenceIds =>
      Source.actorPublisher[EventEnvelope](impl.allPersistenceIds(hints:_*)).asInstanceOf[Source[T,M]]
    case AllEvents =>
      Source.actorPublisher[EventEnvelope](impl.allEvents(hints:_*)).asInstanceOf[Source[T,M]]
    case EventsByPersistenceId(persistenceId, fromSeq, toSeq) =>
      Source.actorPublisher[EventEnvelope](impl.eventsByPersistenceId(persistenceId, fromSeq, toSeq, hints:_*)).asInstanceOf[Source[T,M]]
    case unsupported =>
      failed(q).asInstanceOf[Source[T,M]]
  }

  private def failed[T](q: Query[T,_]) =
    Source.failed[T](new UnsupportedOperationException(s"Query $q not supported by ${getClass.getName}"))

}

trait MongoPersistenceReadJournallingApi {
  def allPersistenceIds(hints: Hint*): Props
  def allEvents(hints: Hint*): Props
  def eventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long, hints: Hint*): Props
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