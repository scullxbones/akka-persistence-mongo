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

class CasbahPersistenceJournaller(driver: CasbahPersistenceDriver) extends MongoPersistenceJournallingApi {
  
  import JournallingFieldNames._

  private[this] implicit def serializeJournal(persistent: PersistentRepr): DBObject =
	    MongoDBObject(PROCESSOR_ID -> persistent.processorId,
	      SEQUENCE_NUMBER -> persistent.sequenceNr,
	      CONFIRMS -> persistent.confirms,
	      DELETED -> persistent.deleted,
	      SERIALIZED -> driver.serialization.serializerFor(classOf[PersistentRepr]).toBinary(persistent))
	
  private[this] implicit def deserializeJournal(document: DBObject): PersistentRepr = {
	    val content = document.as[Array[Byte]](SERIALIZED)
	    val repr = driver.serialization.deserialize(content, classOf[PersistentRepr]).get
	    PersistentRepr(
	      repr.payload,
	      document.as[Long](SEQUENCE_NUMBER),
	      document.as[String](PROCESSOR_ID),
	      document.as[Boolean](DELETED),
	      repr.resolved,
	      repr.redeliveries,
	      document.as[ISeq[String]](CONFIRMS),
	      repr.confirmable,
	      repr.confirmMessage,
	      repr.confirmTarget,
	      repr.sender)
	  }  
  
  private[this] def journalEntryQuery(pid: String, seq: Long) =
    $and(PROCESSOR_ID $eq pid, SEQUENCE_NUMBER $eq seq)

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    $and(PROCESSOR_ID $eq pid, SEQUENCE_NUMBER $gte from $lte to)

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
      documents.foreach { journal.insert(_, WriteConcern.JournalSafe) }
    }
  }.mapTo[Unit]

  private[mongodb] override def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker {
      if (permanent) {
        journal.findAndRemove(journalRangeQuery(pid, from, to))
        	.getOrElse(
        	    throw new RuntimeException(
        	        s"Could not find any journal entries for processor $pid, sequenced from $from to: $to"
        	    )
        	 )
      } else {
        journal.update(journalRangeQuery(pid, from, to), $set(DELETED -> true), false, false, WriteConcern.JournalSafe)
      }
    }
  }.mapTo[Unit]

  private[mongodb] override def confirmJournalEntry(pid: String, seq: Long, channelId: String)(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker {
      journal.update(journalEntryQuery(pid, seq), $push(CONFIRMS -> channelId), false, false, WriteConcern.JournalSafe)
    }
  }.mapTo[Unit]

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = Future {
    driver.breaker.withSyncCircuitBreaker {
      val cursor = journal.find(journalRangeQuery(pid, from, to)).map(deserializeJournal)
      cursor.foreach(replayCallback)
      val maxCursor = journal.find(PROCESSOR_ID $eq pid, 
    		  						MongoDBObject(SEQUENCE_NUMBER -> 1))
    		  				 .sort(MongoDBObject(SEQUENCE_NUMBER -> -1)).limit(1)
      maxCursor.buffered.head.getAs[Long](SEQUENCE_NUMBER).getOrElse(0)
    }
  }

  private[mongodb] def journal(implicit ec: ExecutionContext): MongoCollection = {
    val journalCollection = driver.collection(driver.journalCollectionName)
    journalCollection.ensureIndex(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1, DELETED -> 1),
      MongoDBObject("unique" -> true, "name" -> driver.journalIndexName))
    journalCollection
  }

}