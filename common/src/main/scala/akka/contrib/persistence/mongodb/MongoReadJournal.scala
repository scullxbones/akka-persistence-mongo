package akka.contrib.persistence.mongodb

import akka.actor.{Props, Actor, ExtendedActorSystem}
import akka.contrib.persistence.mongodb.MongoReadJournal.AllEvents
import akka.persistence.query.{EventEnvelope, AllPersistenceIds, Hint, Query}
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.actor.{ActorPublisherMessage, ActorPublisher}
import akka.stream.scaladsl.Source

import scala.concurrent.Future

object MongoReadJournal {
  case object AllEvents extends Query[EventEnvelope, Unit]
}

class MongoReadJournal(system: ExtendedActorSystem) extends ReadJournal {

  private[this] val impl = MongoPersistenceExtension(system).readJournal

  override def query[T, M](q: Query[T, M], hints: Hint*): Source[T, M] = q match {
    case AllPersistenceIds =>
      Source.actorPublisher[EventEnvelope](impl.allPersistenceIds(hints:_*)).asInstanceOf[Source[T,M]]
    case AllEvents =>
      Source.actorPublisher[EventEnvelope](impl.allEvents(hints:_*)).asInstanceOf[Source[T,M]]
    case unsupported =>
      failed(q).asInstanceOf[Source[T,M]]
  }

  private def failed[T](q: Query[T,_]) =
    Source.failed[T](new UnsupportedOperationException(s"Query $q not supported by ${getClass.getName}"))

}

trait MongoPersistenceReadJournallingApi {
  def allPersistenceIds(hints: Hint*): Props
  def allEvents(hints: Hint*): Props
}

trait BufferingActorPublisher[A] extends ActorPublisher[A] {
  import ActorPublisherMessage._

  override def preStart() = {
    context.become(streaming(Vector.empty, 0))
    super.preStart()
  }

  def receive = Actor.emptyBehavior

  def streaming(buf: Vector[A], offset: Long): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded =>
      context.stop(self)
    case Request(_) =>
      val (filled,newOffset) = next(offset)
      val remaining = drainBuf(buf ++ filled)
      checkDone(filled, newOffset, offset, remaining)
      context.become(streaming(remaining, newOffset))
  }

  protected def checkDone(filled: Vector[A], newOffset: Long, oldOffset: Long, remaining: Vector[A]): Unit = {
    if (filled.isEmpty && newOffset == oldOffset && remaining.isEmpty) onCompleteThenStop()
  }

  protected final def drainBuf(buf: Vector[A]): Vector[A] = {
    if (totalDemand > 0 && buf.nonEmpty) {
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        use foreach onNext
        keep
      } else {
        buf foreach onNext
        Vector.empty
      }
    } else buf
  }

  protected def next(previousOffset: Long): (Vector[A], Long)
}

trait NonBlockingBufferingActorPublisher[A] extends ActorPublisher[A] {
  import ActorPublisherMessage._
  import akka.pattern.pipe

  case class More(buf: Vector[A], newOffset: Long)

  override def preStart() = {
    context.become(streaming(Vector.empty, 0))
    super.preStart()
  }

  def receive = Actor.emptyBehavior

  def streaming(buf: Vector[A], offset: Long): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded =>
      context.stop(self)
    case Request(_) =>
      val remaining = drainBuf(buf)
      checkNeedMore(offset)
      context.become(streaming(remaining, offset))
    case More(newBuf,newOffset) =>
      val remaining = drainBuf(buf ++ newBuf)
      checkNeedMore(newOffset)
      checkDone(buf ++ newBuf, newOffset, offset, remaining)
      context.become(streaming(remaining, newOffset))
  }

  protected def checkDone(filled: Vector[A], newOffset: Long, oldOffset: Long, remaining: Vector[A]): Unit = {
    if (filled.isEmpty && newOffset == oldOffset && remaining.isEmpty) onCompleteThenStop()
  }

  protected def checkNeedMore(offset: Long): Unit = {
    implicit val ec = context.dispatcher

    if (totalDemand > 0) next(offset).map{ case(v,o) => More(v,o) }.pipeTo(context.self)
    ()
  }

  protected final def drainBuf(buf: Vector[A]): Vector[A] = {
    if (totalDemand > 0 && buf.nonEmpty) {
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        use foreach onNext
        keep
      } else {
        buf foreach onNext
        Vector.empty
      }
    } else buf
  }

  protected def next(previousOffset: Long): Future[(Vector[A], Long)]
}