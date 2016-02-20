package akka.contrib.persistence.mongodb

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.Actor
import akka.pattern.{CircuitBreaker, CircuitBreakerOpenException}
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.typesafe.config.Config
import nl.grons.metrics.scala.{MetricName, Timer}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

class MongoJournal(config: Config) extends AsyncWriteJournal {
  
  private[this] val impl = MongoPersistenceExtension(context.system)(config).journaler
  private[this] implicit val ec = context.dispatcher

  /**
   * Plugin API: asynchronously writes a batch (`Seq`) of persistent messages to the
   * journal.
   *
   * The batch is only for performance reasons, i.e. all messages don't have to be written
   * atomically. Higher throughput can typically be achieved by using batch inserts of many
   * records compared inserting records one-by-one, but this aspect depends on the
   * underlying data store and a journal implementation can implement it as efficient as
   * possible with the assumption that the messages of the batch are unrelated.
   *
   * Each `AtomicWrite` message contains the single `PersistentRepr` that corresponds to
   * the event that was passed to the `persist` method of the `PersistentActor`, or it
   * contains several `PersistentRepr` that corresponds to the events that were passed
   * to the `persistAll` method of the `PersistentActor`. All `PersistentRepr` of the
   * `AtomicWrite` must be written to the data store atomically, i.e. all or none must
   * be stored. If the journal (data store) cannot support atomic writes of multiple
   * events it should reject such writes with a `Try` `Failure` with an
   * `UnsupportedOperationException` describing the issue. This limitation should
   * also be documented by the journal plugin.
   *
   * If there are failures when storing any of the messages in the batch the returned
   * `Future` must be completed with failure. The `Future` must only be completed with
   * success when all messages in the batch have been confirmed to be stored successfully,
   * i.e. they will be readable, and visible, in a subsequent replay. If there is
   * uncertainty about if the messages were stored or not the `Future` must be completed
   * with failure.
   *
   * Data store connection problems must be signaled by completing the `Future` with
   * failure.
   *
   * The journal can also signal that it rejects individual messages (`AtomicWrite`) by
   * the returned `immutable.Seq[Try[Unit]]`. The returned `Seq` must have as many elements
   * as the input `messages` `Seq`. Each `Try` element signals if the corresponding
   * `AtomicWrite` is rejected or not, with an exception describing the problem. Rejecting
   * a message means it was not stored, i.e. it must not be included in a later replay.
   * Rejecting a message is typically done before attempting to store it, e.g. because of
   * serialization error.
   *
   * Data store connection problems must not be signaled as rejections.
   *
   * Note that it is possible to reduce number of allocations by
   * caching some result `Seq` for the happy path, i.e. when no messages are rejected.
   */
  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] =
    impl.batchAppend(messages)

  /**
   * Plugin API: asynchronously deletes all persistent messages up to `toSequenceNr`
   * (inclusive).
   */
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    impl.deleteFrom(persistenceId, toSequenceNr)

  /**
   * Plugin API
   *
   * Allows plugin implementers to use `f pipeTo self` and
   * handle additional messages for implementing advanced features
   */
  override def receivePluginInternal: Actor.Receive = Actor.emptyBehavior // No advanced features yet.  Stay tuned!

  /**
   * Plugin API: asynchronously replays persistent messages. Implementations replay
   * a message by calling `replayCallback`. The returned future must be completed
   * when all messages (matching the sequence number bounds) have been replayed.
   * The future must be completed with a failure if any of the persistent messages
   * could not be replayed.
   *
   * The `replayCallback` must also be called with messages that have been marked
   * as deleted. In this case a replayed message's `deleted` method must return
   * `true`.
   *
   * The channel ids of delivery confirmations that are available for a replayed
   * message must be contained in that message's `confirms` sequence.
   *
   * @param processorId processor id.
   * @param fromSequenceNr sequence number where replay should start (inclusive).
   * @param toSequenceNr sequence number where replay should end (inclusive).
   * @param max maximum number of messages to be replayed.
   * @param replayCallback called to replay a single message. Can be called from any
   *                       thread.
   */
  override def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit): Future[Unit] = 
  	impl.replayJournal(processorId, fromSequenceNr, toSequenceNr, max)(replayCallback)

  /**
   * Plugin API: asynchronously reads the highest stored sequence number for the
   * given `processorId`.
   *
   * @param processorId processor id.
   * @param fromSequenceNr hint where to start searching for the highest sequence
   *                       number.
   */
  override def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = 
    impl.maxSequenceNr(processorId, fromSequenceNr)

}

trait JournallingFieldNames {
  final val PROCESSOR_ID = "pid"
  final val SEQUENCE_NUMBER = "sn"
  final val CONFIRMS = "cs"
  final val DELETED = "dl"
  final val SERIALIZED = "pr"
  final val MAX_SN = "max_sn"

  final val PayloadKey = "p"
  final val SenderKey = "s"
  final val RedeliveriesKey = "r"
  final val ConfirmableKey = "c"
  final val ConfirmMessageKey = "cm"
  final val ConfirmTargetKey = "ct"

  final val VERSION = "v"
  final val EVENTS = "events"
  final val FROM = "from"
  final val TO = "to"
  final val MANIFEST = "manifest"
  final val WRITER_UUID = "_w"
  final val TYPE = "_t"
  final val HINT = "_h"
  final val SER_MANIFEST = "_sm"
}
object JournallingFieldNames extends JournallingFieldNames

trait MongoPersistenceJournallingApi {
  private[mongodb] def batchAppend(writes: immutable.Seq[AtomicWrite])(implicit ec: ExecutionContext): Future[immutable.Seq[Try[Unit]]]

  private[mongodb] def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit]

  private[mongodb] def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext): Future[Unit]
  
  private[mongodb] def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long]
}

trait MongoPersistenceJournalFailFast extends MongoPersistenceJournallingApi {

  private[mongodb] val breaker: CircuitBreaker

  private lazy val cbOpen = {
    val ab = new AtomicBoolean(false)
    breaker.onOpen(ab.set(true))
    breaker.onHalfOpen(ab.set(false))
    breaker.onClose(ab.set(false))
    ab
  }

  private def onlyWhenClosed[A](thunk: => A) = {
    if (cbOpen.get()) throw new CircuitBreakerOpenException(0.seconds)
    else thunk
  }

  private[mongodb] abstract override def batchAppend(writes: immutable.Seq[AtomicWrite])(implicit ec: ExecutionContext): Future[immutable.Seq[Try[Unit]]] =
    breaker.withCircuitBreaker(super.batchAppend(writes))

  private[mongodb] abstract override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] =
    breaker.withCircuitBreaker(super.deleteFrom(persistenceId, toSequenceNr))

  private[mongodb] abstract override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) =
    onlyWhenClosed(super.replayJournal(pid,from,to,max)(breaker.withSyncCircuitBreaker(replayCallback)))

  private[mongodb] abstract override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext) =
    breaker.withCircuitBreaker(super.maxSequenceNr(pid,from))
}

trait MongoPersistenceJournalMetrics extends MongoPersistenceJournallingApi with Instrumented {
  override lazy val metricBaseName = MetricName(s"akka-persistence-mongo.journal.$driverName")

  def driverName: String
  
  private def timerName(metric: String) = MetricName(metric,"timer").name
  private def histName(metric: String) = MetricName(metric, "histo").name
  
  // Timers
  private val appendTimer = metrics.timer(timerName("write.append"))
  private val deleteTimer = metrics.timer(timerName("write.delete-range"))
  private val replayTimer = metrics.timer(timerName("read.replay"))
  private val maxTimer = metrics.timer(timerName("read.max-seq"))
  
  // Histograms
  private val writeBatchSize = metrics.histogram(histName("write.append.batch-size"))

  private def timeIt[A](timer: Timer)(block: => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    val ctx = timer.timerContext()
    val result = block
    result.onComplete(_ => ctx.stop())
    result
  }
  
  private[mongodb] abstract override def batchAppend(writes: immutable.Seq[AtomicWrite])(implicit ec: ExecutionContext): Future[immutable.Seq[Try[Unit]]] = timeIt (appendTimer) {
    writeBatchSize += writes.map(_.size).sum
    super.batchAppend(writes)
  }

  private[mongodb] abstract override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = timeIt (deleteTimer) {
    super.deleteFrom(persistenceId, toSequenceNr)
  }

  private[mongodb] abstract override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext): Future[Unit]
    = timeIt (replayTimer) { super.replayJournal(pid, from, to, max)(replayCallback) }
  
  private[mongodb] abstract override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long]
    = timeIt (maxTimer) { super.maxSequenceNr(pid, from) }
  
}
  