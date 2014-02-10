package akka.contrib.persistence.mongodb

import akka.persistence.PersistentRepr
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.mongodb.casbah.WriteConcern
import scala.collection.immutable.{Seq => ISeq}
import scala.language.implicitConversions
import akka.actor.ActorSystem
import com.mongodb.casbah.MongoCollection
import akka.serialization.Serialization
import akka.persistence.PersistentConfirmation
import akka.persistence.PersistentId

object CasbahPersistenceJournaller {
  import JournallingFieldNames._
  
  implicit def serializeJournal(persistent: PersistentRepr)(implicit serialization: Serialization): DBObject =
	    MongoDBObject(PROCESSOR_ID -> persistent.processorId,
	      SEQUENCE_NUMBER -> persistent.sequenceNr,
	      CONFIRMS -> persistent.confirms,
	      DELETED -> persistent.deleted,
	      SERIALIZED -> serialization.serializerFor(classOf[PersistentRepr]).toBinary(persistent))
	
  implicit def deserializeJournal(document: DBObject)(implicit serialization: Serialization): PersistentRepr = {
	    val content = document.as[Array[Byte]](SERIALIZED)
	    val repr = serialization.deserialize(content, classOf[PersistentRepr]).get
	    PersistentRepr(
	      repr.payload,
	      document.as[Long](SEQUENCE_NUMBER),
	      document.as[String](PROCESSOR_ID),
	      document.as[Boolean](DELETED),
	      repr.redeliveries,
	      ISeq(document.as[Seq[String]](CONFIRMS) : _*),
	      repr.confirmable,
	      repr.confirmMessage,
	      repr.confirmTarget,
	      repr.sender)
	  }  
  
}

class CasbahPersistenceJournaller(driver: CasbahPersistenceDriver) extends MongoPersistenceJournallingApi {
  
  import JournallingFieldNames._
  import CasbahPersistenceJournaller._
  
  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  private[this] def journalEntryQuery(pid: String, seq: Long) =
    $and(PROCESSOR_ID $eq pid, SEQUENCE_NUMBER $eq seq)

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    $and(PROCESSOR_ID $eq pid, $and(SEQUENCE_NUMBER $gte from, SEQUENCE_NUMBER $lte to))

  private[mongodb] override def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker {
      journal.findOne(journalEntryQuery(pid, seq)).map(deserializeJournal)
    }
  }

  private[mongodb] override def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker {
      val cursor = journal.find(journalRangeQuery(pid, from, to))
      cursor.buffered.map(deserializeJournal)
    }
  }

  private[mongodb] override def appendToJournal(documents: TraversableOnce[PersistentRepr])(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker {
      documents.foreach { journal.insert(_, writeConcern) }
    }
  }.mapTo[Unit]
  
  private[this] def hardOrSoftDelete(query: DBObject, hard: Boolean)(implicit ec: ExecutionContext) =
      if (hard) {
        journal.remove(query, writeConcern)
      } else {
        journal.update(query, $set(DELETED -> true), false, true, writeConcern)
      }

  private[mongodb] override def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker(hardOrSoftDelete(journalRangeQuery(pid, from, to), permanent))
  }.mapTo[Unit]

  private[mongodb] override def confirmJournalEntries(confirms: ISeq[PersistentConfirmation])(implicit ec: ExecutionContext): Future[Unit] = 
  	Future.reduce(confirms.map( c => confirmJournalEntry(c.processorId, c.sequenceNr, c.channelId) ))((r,u) => u)
  
  private[mongodb] override def deleteAllMatchingJournalEntries(ids: ISeq[PersistentId],permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit] =
    driver.breaker.withCircuitBreaker {
      Future.reduce(ids.map (id => Future {
        hardOrSoftDelete(journalEntryQuery(id.processorId,id.sequenceNr), permanent)
      }.mapTo[Unit]))((r,u) => u)
	}
    
  
  private[mongodb] def maxSequenceNr(pid: String,from: Long)(implicit ec: ExecutionContext): Future[Long] = Future {
    driver.breaker.withSyncCircuitBreaker {
      val maxCursor = journal.find($and(PROCESSOR_ID $eq pid, SEQUENCE_NUMBER $gte from), 
    		  						MongoDBObject(SEQUENCE_NUMBER -> 1))
    		  				 .sort(MongoDBObject(SEQUENCE_NUMBER -> -1)).limit(1)
      maxCursor.buffered.head.getAs[Long](SEQUENCE_NUMBER).getOrElse(0)
    }
  }
  
  private[this] def confirmJournalEntry(pid: String, seq: Long, channelId: String)(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker {
      journal.update(journalEntryQuery(pid, seq), $push(CONFIRMS -> channelId), false, false, writeConcern)
    }
  }.mapTo[Unit]

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker {
      val cursor = journal.find(journalRangeQuery(pid, from, to)).map(deserializeJournal)
      cursor.foreach(replayCallback)
    }
  }

  private[mongodb] def journal(implicit ec: ExecutionContext): MongoCollection = {
    val journalCollection = driver.collection(driver.journalCollectionName)
    journalCollection.ensureIndex(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1, DELETED -> 1),
      MongoDBObject("unique" -> true, "name" -> driver.journalIndexName))
    journalCollection
  }

}