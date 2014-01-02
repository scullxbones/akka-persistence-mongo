package akka.contrib.persistence.mongodb

import scala.collection.immutable.Seq
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.PersistentRepr
import scala.concurrent.Future

class MongoJournal extends AsyncWriteJournal {
  
  private[this] val impl = context.system.extension(MongoPersistenceExtensionId)
  private[this] implicit val ec = context.dispatcher

  /**
   * Plugin API: asynchronously writes a batch of persistent messages to the journal.
   * The batch write must be atomic i.e. either all persistent messages in the batch
   * are written or none.
   */
  override def writeAsync(persistentBatch: Seq[PersistentRepr]): Future[Unit] = 
    impl.appendToJournal(persistentBatch)

  /**
   * Plugin API: asynchronously deletes all persistent messages within the range from
   * `fromSequenceNr` to `toSequenceNr` (both inclusive). If `permanent` is set to
   * `false`, the persistent messages are marked as deleted, otherwise they are
   * permanently deleted.
   *
   * @see [[AsyncReplay]]
   */
  override def deleteAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = 
    impl.deleteJournalEntries(processorId, fromSequenceNr, toSequenceNr, permanent)

  /**
   * Plugin API: asynchronously writes a delivery confirmation to the journal.
   */
  override def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = 
    impl.confirmJournalEntry(processorId, sequenceNr, channelId)

  /**
   * Plugin API: asynchronously replays persistent messages. Implementations replay
   * a message by calling `replayCallback`. The returned future must be completed
   * when all messages (matching the sequence number bounds) have been replayed. The
   * future `Long` value must be the highest stored sequence number in the journal
   * for the specified processor. The future must be completed with a failure if any
   * of the persistent messages could not be replayed.
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
   * @param replayCallback called to replay a single message. Can be called from any
   *                       thread.
   *
   * @see [[AsyncWriteJournal]]
   * @see [[SyncWriteJournal]]
   */
  override def replayAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: PersistentRepr ⇒ Unit): Future[Long] = {
	  impl.replayJournal(processorId, fromSequenceNr, toSequenceNr)(replayCallback)
  }

}