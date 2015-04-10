package akka.contrib.persistence.mongodb

import akka.actor.ActorRef
import akka.persistence._
import play.api.libs.iteratee.{Enumerator, Iteratee}
import reactivemongo.api.indexes._
import reactivemongo.bson._

import scala.collection.immutable.{Seq => ISeq}
import scala.concurrent._

class RxMongoJournaller(driver: RxMongoPersistenceDriver) extends MongoPersistenceJournallingApi {

  import JournallingFieldNames._
  import RxMongoPersistenceExtension._

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  implicit object PersistentReprHandler extends BSONDocumentReader[PersistentRepr] with BSONDocumentWriter[PersistentRepr] {
    val PayloadKey = "payload"
    val SenderKey = "sender"

    def read(document: BSONDocument): PersistentRepr = {
      val repr: PersistentRepr = document.get(SERIALIZED).get match {
        case b: BSONDocument =>
          PersistentRepr(
            payload = b.get(PayloadKey).get,
            sender = ActorRef.noSender
          )
        case v: BSONBinary =>
          serialization.deserialize(BsonBinaryHandler.read(v), classOf[PersistentRepr]).get
      }

      val confirms = ISeq(document.getAs[Seq[String]](CONFIRMS).getOrElse(Seq.empty[String]): _*)

      PersistentRepr(
        payload = repr.payload,
        sequenceNr = document.getAs[Long](SEQUENCE_NUMBER).get,
        persistenceId = document.getAs[String](PROCESSOR_ID).get,
        deleted = document.getAs[Boolean](DELETED).getOrElse(false),
        redeliveries = repr.redeliveries,
        confirms = confirms,
        confirmable = repr.confirmable,
        confirmMessage = repr.confirmMessage,
        confirmTarget = repr.confirmTarget,
        sender = repr.sender)
    }

    def write(persistent: PersistentRepr): BSONDocument = {
      val content: BSONValue = persistent.payload match {
        case b: BSONDocument =>
          BSONDocument(
            PayloadKey -> b,
            SenderKey -> persistent.sender.path.toSerializationFormat
          )
        case _ =>
          BsonBinaryHandler.write(serialization.serialize(persistent).get)
      }

      BSONDocument(PROCESSOR_ID -> persistent.processorId,
        SEQUENCE_NUMBER -> persistent.sequenceNr,
        DELETED -> persistent.deleted,
        CONFIRMS -> persistent.confirms,
        SERIALIZED -> content)
    }
  }

  private[this] def journalEntryQuery(pid: String, seq: Long) =
    BSONDocument(PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq)

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    BSONDocument(PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> BSONDocument("$gte" -> from, "$lte" -> to))

  private[mongodb] override def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext) =
    journal.find(journalEntryQuery(pid, seq)).one[PersistentRepr]

  private[mongodb] override def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) =
    journal.find(journalRangeQuery(pid, from, to)).cursor[PersistentRepr].collect[Vector]().map(_.iterator.asInstanceOf[Iterator[PersistentRepr]])

  private[mongodb] override def appendToJournal(persistent: TraversableOnce[PersistentRepr])(implicit ec: ExecutionContext) =
    journal.bulkInsert(Enumerator.enumerate(persistent), writeConcern).map(_ => ())

  private[this] def hardOrSoftDelete(query: BSONDocument, permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit] = {
    val result =
      if (permanent) {
        journal.remove(query, writeConcern)
      } else {
        journal.update(query, BSONDocument("$set" -> BSONDocument(DELETED -> true)), writeConcern, multi = true)
      }
    result.map(_ => ())
  }

  private[mongodb] override def deleteAllMatchingJournalEntries(ids: ISeq[PersistentId], permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit] = {
    Future.reduce(ids.map { pi =>
      hardOrSoftDelete(journalEntryQuery(pi.processorId, pi.sequenceNr), permanent)
    })((r, u) => u)
  }

  private[mongodb] override def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext) = {
    val query = journalRangeQuery(pid, from, to)
    hardOrSoftDelete(query, permanent)
  }

  private[mongodb] override def confirmJournalEntries(confirms: ISeq[PersistentConfirmation])(implicit ec: ExecutionContext) = {
    val grouped = confirms.groupBy(c => (c.persistenceId, c.sequenceNr))

    Future.reduce(grouped.map { case ((id, seq), groupedConfirms) =>
      val channels = groupedConfirms.map(c => c.channelId).toSeq
      val update = BSONDocument("$push" -> BSONDocument(CONFIRMS -> BSONDocument("$each" -> channels)))
      journal.update(
        journalEntryQuery(id, seq),
        update,
        writeConcern
      ).map(_ => ())
    })((u, t) => u)
  }


  private[mongodb] override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext) =
    journal.find(BSONDocument(PROCESSOR_ID -> pid))
      .sort(BSONDocument(SEQUENCE_NUMBER -> -1))
      .cursor[PersistentRepr]
      .headOption
      .map(l => l.map(_.sequenceNr).getOrElse(0L))

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) =
    if (max == 0L) Future.successful(())
    else {
      val cursor = journal
        .find(journalRangeQuery(pid, from, to))
        .cursor[PersistentRepr]

      val maxInt = toInt(max)

      val it = Iteratee.fold2[PersistentRepr, Int](maxInt) {
        case (remaining, el) =>
          replayCallback(el)
          Future.successful((remaining - 1, remaining <= 1))
      }
      cursor.enumerate(maxInt, stopOnError = true).run(it).map(_ => ())
    }

  private[this] def toInt(long: Long): Int = {
    if (long > Int.MaxValue) {
      Int.MaxValue
    } else {
      long.intValue
    }
  }

  private[this] def journal(implicit ec: ExecutionContext) = {
    val journal = driver.collection(driver.journalCollectionName)
    journal.indexesManager.ensure(new Index(
      key = Seq((PROCESSOR_ID, IndexType.Ascending),
        (SEQUENCE_NUMBER, IndexType.Ascending),
        (DELETED, IndexType.Ascending)),
      background = true,
      unique = true,
      name = Some(driver.journalIndexName)))
    journal
  }

}