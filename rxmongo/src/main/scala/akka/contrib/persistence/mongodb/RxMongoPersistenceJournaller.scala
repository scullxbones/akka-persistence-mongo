package akka.contrib.persistence.mongodb

import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.api.indexes._
import reactivemongo.bson._
import reactivemongo.bson.buffer.ArrayReadableBuffer
import reactivemongo.core.commands._
import akka.persistence.PersistentRepr
import scala.collection.immutable.{ Seq => ISeq }
import scala.concurrent._
import play.api.libs.iteratee.Iteratee
import akka.persistence.PersistentConfirmation
import akka.persistence.PersistentId

class RxMongoJournaller(driver: RxMongoPersistenceDriver) extends MongoPersistenceJournallingApi {

  import RxMongoPersistenceExtension._
  import JournallingFieldNames._

  implicit object PersistentReprHandler extends BSONDocumentReader[PersistentRepr] with BSONDocumentWriter[PersistentRepr] {
    def read(document: BSONDocument): PersistentRepr = {
      val content = document.getAs[Array[Byte]](SERIALIZED).get
      val repr = driver.serialization.deserialize(content, classOf[PersistentRepr]).get
      PersistentRepr(
        repr.payload,
        document.getAs[Long](SEQUENCE_NUMBER).get,
        document.getAs[String](PROCESSOR_ID).get,
        document.getAs[Boolean](DELETED).get,
        repr.redeliveries,
        document.getAs[ISeq[String]](CONFIRMS).get,
        repr.confirmable,
        repr.confirmMessage,
        repr.confirmTarget,
        repr.sender)
    }

    def write(persistent: PersistentRepr): BSONDocument = {
      val content = driver.serialization.serialize(persistent).get
      BSONDocument(PROCESSOR_ID -> persistent.processorId,
        SEQUENCE_NUMBER -> persistent.sequenceNr,
        DELETED -> persistent.deleted,
        CONFIRMS -> BSONArray(persistent.confirms),
        SERIALIZED -> content)
    }
  }

  private[this] def journalEntryQuery(pid: String, seq: Long) =
    BSONDocument(PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq)

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    BSONDocument(PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> BSONDocument("$gte" -> from, "$lte" -> to))

  private[this] def modifyJournalEntry(query: BSONDocument, op: BSONDocument)(implicit ec: ExecutionContext) =
    journal.db.command(
      new FindAndModify(journal.name, query, Update(op, false)))

  private[mongodb] override def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext) =
    journal.find(journalEntryQuery(pid, seq)).one[PersistentRepr]

  private[mongodb] override def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) =
    journal.find(journalRangeQuery(pid, from, to)).cursor[PersistentRepr].collect[Vector]().map(_.iterator)

  private[mongodb] override def appendToJournal(persistent: TraversableOnce[PersistentRepr])(implicit ec: ExecutionContext) =
    Future.reduce(persistent.map { doc => journal.insert(doc).mapTo[Unit] })((u, gle) => u)

  private[this] def hardOrSoftDelete(query: BSONDocument, permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit] = {
    val result =
      if (permanent) {
        journal.remove(query)
      } else {
        modifyJournalEntry(query, BSONDocument("$set" -> BSONDocument(DELETED -> true)))
      }
    result.mapTo[Unit]
  }  
    
  private[mongodb] override def deleteAllMatchingJournalEntries(ids: ISeq[PersistentId], permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit] = {
    Future.reduce(ids.map { pi =>
      hardOrSoftDelete(journalEntryQuery(pi.processorId, pi.sequenceNr), permanent)
    })((r,u) => u)
  }  
    
  private[mongodb] override def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext) = {
    val query = journalRangeQuery(pid, from, to)
    hardOrSoftDelete(query, permanent)
  }

  private[mongodb] override def confirmJournalEntries(confirms: ISeq[PersistentConfirmation])(implicit ec: ExecutionContext) =
    Future.reduce( confirms.map { pc =>
      modifyJournalEntry(
        journalEntryQuery(pc.processorId, pc.sequenceNr),
        BSONDocument("$push" -> BSONDocument(CONFIRMS -> pc.channelId))
      ).map(_.getOrElse(BSONDocument())).mapTo[Unit]
    } )((u,t) => u)
  
  
  private[mongodb] override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext) = {
    journal.find(BSONDocument(PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> BSONDocument("$gte" -> from)))
      .sort(BSONDocument(SEQUENCE_NUMBER -> -1))
      .cursor[PersistentRepr]
      .collect[List](1)
      .map(l => l.headOption.map(pi => pi.sequenceNr).getOrElse(0L))
  }

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = {
    val cursor = journal
      .find(journalRangeQuery(pid, from, to))
      .cursor[PersistentRepr]
    val result = cursor.enumerate().apply(Iteratee.foreach { p =>
      replayCallback(p)
    }).mapTo[Unit]
    result
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