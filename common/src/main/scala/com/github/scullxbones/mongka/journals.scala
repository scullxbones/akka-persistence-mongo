package com.github.scullxbones.mongka

import akka.persistence._
import akka.serialization._
import reactivemongo.bson._
import akka.persistence.journal.AsyncWriteJournal
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import play.api.libs.iteratee.Iteratee
import reactivemongo.core.commands._
import akka.actor.ActorLogging

class MongkaJournal extends AsyncWriteJournal with ActorLogging {

  import serializers._

  private[this] val serialization = SerializationExtension(context.system)
  private[this] val extension = context.system.extension(MongkaExtensionId)
  private[this] val journal = extension.journal
  private[this] val breaker = extension.breaker

  implicit object PersistentImplHandler extends BSONDocumentReader[PersistentImpl] with BSONDocumentWriter[PersistentImpl] {
    def read(doc: BSONDocument): PersistentImpl = {
      val content = doc.getAs[Array[Byte]]("pi").get
      val impl = serialization.deserialize(content, classOf[PersistentImpl]).get
      impl.copy(deleted = doc.getAs[Boolean]("dl").get, confirms = doc.getAs[Seq[String]]("cs").get)
    }

    def write(persistent: PersistentImpl): BSONDocument = {
      val content = serialization.serialize(persistent).get
      BSONDocument("pid" -> persistent.processorId,
        "sq" -> persistent.sequenceNr,
        "dl" -> persistent.deleted,
        "cs" -> BSONArray(persistent.confirms),
        "pi" -> content)
    }
  }

  private[this] def journalEntry(pid: String, seq: Long) =
    BSONDocument("pid" -> pid, "sn" -> seq)

  private[this] def journalRange(pid: String, from: Long, to: Long) =
    BSONDocument("pid" -> pid, "sn" -> BSONDocument("$gte" -> from, "$lte" -> to))

  private[this] def modifyJournalEntry(pid: String, seq: Long, op: BSONDocument) =
      journal.db.command(
        new FindAndModify(journal.name, journalEntry(pid, seq), Update(op, false))
      )

  /**
   * Plugin API.
   *
   * Asynchronously writes a `persistent` message to the journal.
   */
  override def writeAsync(persistent: PersistentImpl): Future[Unit] = breaker.withCircuitBreaker {
    journal.insert(persistent).map { le => log.error(le.toString()) }
  }

  /**
   * Plugin API.
   *
   * Asynchronously marks a `persistent` message as deleted.
   */
  override def deleteAsync(persistent: PersistentImpl): Future[Unit] = breaker.withCircuitBreaker {
      modifyJournalEntry(persistent.processorId, persistent.sequenceNr,
        BSONDocument("$set" -> BSONDocument("dl" -> true))).map { le => log.error(le.toString()) }
  }

  /**
   * Plugin API.
   *
   * Asynchronously writes a delivery confirmation to the journal.
   */
  override def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = breaker.withCircuitBreaker {
      modifyJournalEntry(processorId, sequenceNr,
        BSONDocument("$push" -> BSONDocument("cs" -> channelId))).map { le => log.error(le.toString()) }
  }

  /**
   * Plugin API.
   *
   * Asynchronously replays persistent messages. Implementations replay a message
   * by calling `replayCallback`. The returned future must be completed when all
   * messages (matching the sequence number bounds) have been replayed. The future
   * `Long` value must be the highest stored sequence number in the journal for the
   * specified processor. The future must be completed with a failure if any of
   * the persistent messages could not be replayed.
   *
   * The `replayCallback` must also be called with messages that have been marked
   * as deleted. In this case a replayed message's `deleted` field must be set to
   * `true`.
   *
   * The channel ids of delivery confirmations that are available for a replayed
   * message must be contained in that message's `confirms` sequence.
   *
   * @param processorId processor id.
   * @param fromSequenceNr sequence number where replay should start.
   * @param toSequenceNr sequence number where replay should end (inclusive).
   * @param replayCallback called to replay a single message.
   *
   * @see [[AsyncWriteJournal]]
   * @see [[SyncWriteJournal]]
   */
  override def replayAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: PersistentImpl ⇒ Unit): Future[Long] =
    breaker.withCircuitBreaker {
      val cursor = journal
        .find(journalRange(processorId, fromSequenceNr, toSequenceNr))
        .cursor[PersistentImpl]
      val result: Future[Long] = cursor.enumerate().apply(Iteratee.foreach { p =>
        replayCallback(p)
      }).flatMap(_ => {
        val max = Aggregate(journal.name,
          Seq(Match(BSONDocument("pid" -> processorId)),
            GroupField("pid")(("sn", Max("sn")))))
        journal.db.command(max)
      }).map { x => x.headOption.flatMap{ doc => doc.getAs[Long]("result") }.getOrElse(0) }
      result
    }
}
