package akka.contrib.persistence.mongodb

import akka.actor.ExtendedActorSystem
import com.mongodb.casbah.Imports._
import akka.persistence.PersistentRepr
import scala.concurrent.Future
import akka.serialization.SerializationExtension
import com.mongodb.ServerAddress
import com.mongodb.casbah.WriteConcern
import scala.collection.immutable.{ Seq => ISeq }
import scala.concurrent.ExecutionContext
import akka.persistence.SelectedSnapshot
import scala.language.implicitConversions

trait CasbahPersistenceBase extends MongoPersistenceBase {
  // Document type
  type D = DBObject
  // Collection type
  type C = MongoCollection

  val serialization = SerializationExtension(actorSystem)

  val client = MongoClient(mongoUrl.map { url =>
    val Array(host, port) = url.split(":")
    new ServerAddress(host, port.toInt)
  }.toList)
  val db = client(mongoDbName)

  private[mongodb] override def collection(name: String)(implicit ec: ExecutionContext): C = db(name)

  implicit def serializeJournal(persistent: PersistentRepr): D =
	    MongoDBObject("pid" -> persistent.processorId,
	      "sn" -> persistent.sequenceNr,
	      "cs" -> persistent.confirms,
	      "dl" -> persistent.deleted,
	      "pr" -> serialization.serializerFor(classOf[PersistentRepr]).toBinary(persistent))
	
  implicit def deserializeJournal(document: D): PersistentRepr = {
	    val content = document.as[Array[Byte]]("pr")
	    val repr = serialization.deserialize(content, classOf[PersistentRepr]).get
	    PersistentRepr(
	      repr.payload,
	      document.as[Long]("sn"),
	      document.as[String]("pid"),
	      document.as[Boolean]("dl"),
	      repr.resolved,
	      repr.redeliveries,
	      document.as[ISeq[String]]("cs"),
	      repr.confirmable,
	      repr.confirmMessage,
	      repr.confirmTarget,
	      repr.sender)
	  }
  
   implicit def serializeSnapshot(snapshot: SelectedSnapshot): D =
	    MongoDBObject("pid" -> snapshot.metadata.processorId,
	      "sn" -> snapshot.metadata.sequenceNr,
	      "ts" -> snapshot.metadata.timestamp,
	      "ss" -> serialization.serializerFor(classOf[SelectedSnapshot]).toBinary(snapshot))
	      
   implicit def deserializeSnapshot(document: D): SelectedSnapshot = {
     val content = document.as[Array[Byte]]("ss")
     serialization.deserialize(content, classOf[SelectedSnapshot]).get
   }
	      
}

trait CasbahPersistenceJournalling extends MongoPersistenceJournalling with CasbahPersistenceBase {
  private[this] def journalEntryQuery(pid: String, seq: Long) =
    $and("pid" $eq pid, "sn" $eq seq)

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    $and("pid" $eq pid, "sn" $gte from $lte to)

  private[mongodb] override def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext) = Future {
    breaker.withSyncCircuitBreaker {
      journal.findOne(journalEntryQuery(pid, seq)).map(deserializeJournal)
    }
  }

  private[mongodb] override def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) = Future {
    breaker.withSyncCircuitBreaker {
      val cursor = journal.find(journalRangeQuery(pid, from, to))
      cursor.buffered.map(deserializeJournal)
    }
  }

  private[mongodb] override def appendToJournal(documents: TraversableOnce[PersistentRepr])(implicit ec: ExecutionContext) = Future {
    breaker.withSyncCircuitBreaker {
      documents.foreach { journal.insert(_, WriteConcern.JournalSafe) }
    }
  }

  private[mongodb] override def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext) = Future {
    breaker.withSyncCircuitBreaker {
      if (permanent) {
        journal.findAndRemove(journalRangeQuery(pid, from, to))
        	.getOrElse(throw new RuntimeException(s"Could not find any journal entries for processor $pid, sequenced from $from to: $to"))
      } else {
        journal.update(journalRangeQuery(pid, from, to), $set("dl" -> true), false, false, WriteConcern.JournalSafe)
      }
      ()
    }
  }

  private[mongodb] override def confirmJournalEntry(pid: String, seq: Long, channelId: String)(implicit ec: ExecutionContext) = Future {
    breaker.withSyncCircuitBreaker {
      journal.update(journalEntryQuery(pid, seq), $push("cs" -> channelId), false, false, WriteConcern.JournalSafe)
      ()
    }
  }

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = Future {
    breaker.withSyncCircuitBreaker {
      val cursor = journal.find(journalRangeQuery(pid, from, to)).map(deserializeJournal)
      cursor.foreach(replayCallback)
      val maxCursor = journal.find("pid" $eq pid, MongoDBObject("sn" -> 1)).sort(MongoDBObject("sn" -> -1)).limit(1)
      maxCursor.buffered.head.getAs[Long]("sn").getOrElse(0)
    }
  }

  private[mongodb] override def journal(implicit ec: ExecutionContext): C = {
    val collection = db(settings.JournalCollection)
    collection.ensureIndex(MongoDBObject("pid" -> 1, "sq" -> 1, "dl" -> 1),
      MongoDBObject("unique" -> true, "name" -> settings.JournalIndex))
    collection
  }

}

trait CasbahPersistenceSnapshotting extends MongoPersistenceSnapshotting with CasbahPersistenceBase {
  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = Future {
    breaker.withSyncCircuitBreaker {
      snaps.find($and("pid" $eq pid, "sq" $lte maxSeq, "ts" $lte maxTs))
        .sort(MongoDBObject("sq" -> -1, "ts" -> -1))
        .limit(1)
        .collectFirst {
          case o: MongoDBObject => deserializeSnapshot(o)
        }
    }
  }

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) = Future {
    breaker.withSyncCircuitBreaker { snaps.insert(snapshot, WriteConcern.JournalSafe) }
  }
  
  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) = breaker.withSyncCircuitBreaker {
    snaps.remove($and("pid" $eq pid, "sq" $eq seq, "ts" $eq ts), WriteConcern.JournalSafe)
  }
  
  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = breaker.withSyncCircuitBreaker {
    snaps.remove($and("pid" $eq pid, "sq" $lte maxSeq, "ts" $lte maxTs), WriteConcern.JournalSafe)
  }
  
  
  private[mongodb] override def snaps(implicit ec: ExecutionContext): C = {
    val collection = db(settings.SnapsCollection)
    collection.ensureIndex(MongoDBObject("pid" -> 1, "sq" -> -1, "ts" -> -1),
      MongoDBObject("unique" -> true, "name" -> settings.SnapsIndex))
    collection
  }
}

class CasbahPersistenceExtension(val actorSystem: ExtendedActorSystem) 
	extends MongoPersistenceExtension 
	with CasbahPersistenceJournalling 
	with CasbahPersistenceSnapshotting