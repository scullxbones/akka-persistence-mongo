package akka.contrib.persistence.mongodb

import reactivemongo.api.indexes._
import reactivemongo.bson._
import akka.persistence.PersistentRepr
import scala.collection.immutable.{ Seq => ISeq }
import scala.concurrent._
import play.api.libs.iteratee.{Enumerator, Iteratee}
import akka.persistence.PersistentConfirmation
import akka.persistence.PersistentId

import scala.util.{Failure, Success}

class RxMongoJournaller(driver: RxMongoPersistenceDriver) extends MongoPersistenceJournallingApi {

  import RxMongoPersistenceExtension._
  import JournallingFieldNames._

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  implicit object PersistentReprHandler extends BSONDocumentReader[PersistentRepr] with BSONDocumentWriter[PersistentRepr] {
    def read(document: BSONDocument): PersistentRepr = {
      val content = document.getAsTry[Array[Byte]](SERIALIZED)
      val repr = serialization.deserialize(content.get, classOf[PersistentRepr]).get
      val confirms = ISeq(document.getAs[Seq[String]](CONFIRMS).getOrElse(Seq.empty[String]):_*)
      PersistentRepr(
        repr.payload,
        document.getAs[Long](SEQUENCE_NUMBER).get,
        document.getAs[String](PROCESSOR_ID).get,
        document.getAs[Boolean](DELETED).getOrElse(false),
        repr.redeliveries,
        confirms,
        repr.confirmable,
        repr.confirmMessage,
        repr.confirmTarget,
        repr.sender)
    }

    def write(persistent: PersistentRepr): BSONDocument = {
      val content = serialization.serialize(persistent).get
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
    journal.bulkInsert(Enumerator.enumerate(persistent),writeConcern).map(_ => ())

  private[this] def hardOrSoftDelete(query: BSONDocument, permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit] = {
    val result =
      if (permanent) {
        journal.remove(query,writeConcern)
      } else {
        journal.update(query, BSONDocument("$set" -> BSONDocument(DELETED -> true)),writeConcern,multi = true)
      }
    result.map(_ => ())
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

  private[mongodb] override def confirmJournalEntries(confirms: ISeq[PersistentConfirmation])(implicit ec: ExecutionContext) = {
    val grouped = confirms.groupBy(c => (c.persistenceId,c.sequenceNr))

    Future.reduce( grouped.map { case((id,seq),groupedConfirms) =>
      val channels = groupedConfirms.map(c => c.channelId).toSeq
      val update = BSONDocument("$push" -> BSONDocument(CONFIRMS -> BSONDocument("$each" -> channels)))
      journal.update(
        journalEntryQuery(id, seq),
        update,
        writeConcern
      ).map(_ => ())
    } )((u,t) => u)
  }

  
  private[mongodb] override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext) =
    journal.find(BSONDocument(PROCESSOR_ID -> pid))
            .sort(BSONDocument(SEQUENCE_NUMBER -> -1))
            .cursor[PersistentRepr]
            .headOption
            .map(l => l.map(_.sequenceNr).getOrElse(0L))

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) =
    if (max == 0L) Promise().success(()).future
    else {
      val it = Iteratee.fold2[PersistentRepr,Long](max){ case(remaining,el) =>
        replayCallback(el)
        Promise[(Long,Boolean)]().success((remaining-1,remaining <= 1)).future
      }
      val cursor = journal
        .find(journalRangeQuery(pid, from, to))
        .cursor[PersistentRepr]
      cursor.enumerate(max.toInt,stopOnError = true).run(it).map(_ => ())
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