package akka.contrib.persistence.mongodb

import akka.actor.{ActorSystem, ActorRef}
import akka.persistence.{Delivered, PersistentConfirmation, PersistentId, PersistentRepr}
import akka.serialization.Serialization
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection

import scala.annotation.tailrec
import scala.collection.immutable.{Seq => ISeq}
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

object CasbahPersistenceJournaller {

  import JournallingFieldNames._

  implicit def serializeJournal(persistent: PersistentRepr)(implicit serialization: Serialization, system: ActorSystem): DBObject = {
    val content = persistent.payload match {
      case dbo: DBObject =>
        val o = MongoDBObject(
          PayloadKey -> dbo
        )
        Seq {
          Option(persistent.sender).filterNot(_ == system.deadLetters).flatMap(serialization.serialize(_).toOption).map(SenderKey -> _)
          Option(persistent.redeliveries).filterNot(_ == 0).map(RedeliveriesKey -> _)
          Option(persistent.confirmable).filter(identity).map(ConfirmableKey -> _)
          Option(persistent.confirmMessage).flatMap(serialization.serialize(_).toOption).map(ConfirmMessageKey -> _)
          Option(persistent.confirmTarget).flatMap(serialization.serialize(_).toOption).map(ConfirmTargetKey -> _)
        }.collect {
          case Some(bb) => bb
        }.foreach(kv => o.put(kv._1, kv._2))
        o
      case _ =>
        serialization.serializerFor(classOf[PersistentRepr]).toBinary(persistent)
    }
    MongoDBObject(PROCESSOR_ID -> persistent.processorId,
      SEQUENCE_NUMBER -> persistent.sequenceNr,
      CONFIRMS -> persistent.confirms,
      DELETED -> persistent.deleted,
      SERIALIZED -> content)
  }

  implicit def deserializeJournal(document: DBObject)(implicit serialization: Serialization, system: ActorSystem): PersistentRepr = {
    document.get(SERIALIZED) match {
      case b: DBObject =>
        PersistentRepr(
          payload = b.as[DBObject](PayloadKey),
          sequenceNr = document.as[Long](SEQUENCE_NUMBER),
          persistenceId = document.as[String](PROCESSOR_ID),
          deleted = document.as[Boolean](DELETED),
          redeliveries = b.getAs[Int](RedeliveriesKey).getOrElse(0),
          confirms = ISeq(document.as[Seq[String]](CONFIRMS): _*),
          confirmable = b.getAs[Boolean](ConfirmableKey).getOrElse(false),
          confirmMessage = b.getAs[Array[Byte]](ConfirmMessageKey).flatMap(serialization.deserialize(_, classOf[Delivered]).toOption).orNull,
          confirmTarget = b.getAs[Array[Byte]](ConfirmTargetKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption).orNull,
          sender = b.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption).getOrElse(system.deadLetters)
        )
      case _ =>
        val content = document.as[Array[Byte]](SERIALIZED)
        val repr = serialization.deserialize(content, classOf[PersistentRepr]).get
        PersistentRepr(
          repr.payload,
          document.as[Long](SEQUENCE_NUMBER),
          document.as[String](PROCESSOR_ID),
          document.as[Boolean](DELETED),
          repr.redeliveries,
          ISeq(document.as[Seq[String]](CONFIRMS): _*),
          repr.confirmable,
          repr.confirmMessage,
          repr.confirmTarget,
          repr.sender)
    }
  }

}

class CasbahPersistenceJournaller(driver: CasbahPersistenceDriver) extends MongoPersistenceJournallingApi {

  import CasbahPersistenceJournaller._
  import JournallingFieldNames._

  implicit val system = driver.actorSystem

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  private[this] def journalEntryQuery(pid: String, seq: Long) =
    $and(PROCESSOR_ID $eq pid, SEQUENCE_NUMBER $eq seq)

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    $and(PROCESSOR_ID $eq pid, $and(SEQUENCE_NUMBER $gte from, SEQUENCE_NUMBER $lte to))

  private[mongodb] override def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext) = Future {
    journal.findOne(journalEntryQuery(pid, seq)).map(deserializeJournal)
  }

  private[mongodb] override def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) = Future {
    val cursor = journal.find(journalRangeQuery(pid, from, to))
    cursor.buffered.map(deserializeJournal)
  }

  private[mongodb] override def appendToJournal(documents: TraversableOnce[PersistentRepr])(implicit ec: ExecutionContext) = Future {
    journal.insert(documents.toSeq: _*)(serializeJournal, writeConcern)
    ()
  }.mapTo[Unit]

  private[this] def hardOrSoftDelete(query: DBObject, hard: Boolean)(implicit ec: ExecutionContext): Unit =
    if (hard) {
      journal.remove(query, writeConcern)
    } else {
      journal.update(query, $set(DELETED -> true), false, true, writeConcern)
    }

  private[mongodb] override def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext) = Future {
    hardOrSoftDelete(journalRangeQuery(pid, from, to), permanent)
  }.mapTo[Unit]

  private[mongodb] override def confirmJournalEntries(confirms: ISeq[PersistentConfirmation])(implicit ec: ExecutionContext): Future[Unit] =
    Future.reduce(confirms.map(c => confirmJournalEntry(c.processorId, c.sequenceNr, c.channelId)))((r, u) => u)

  private[mongodb] override def deleteAllMatchingJournalEntries(ids: ISeq[PersistentId], permanent: Boolean)(implicit ec: ExecutionContext): Future[Unit] =
    Future.reduce(ids.map(id => Future {
      hardOrSoftDelete(journalEntryQuery(id.processorId, id.sequenceNr), permanent)
    }.mapTo[Unit]))((r, u) => u)


  private[mongodb] def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long] = Future {
    val maxCursor = journal.find($and(PROCESSOR_ID $eq pid),
      MongoDBObject(SEQUENCE_NUMBER -> 1))
      .sort(MongoDBObject(SEQUENCE_NUMBER -> -1)).limit(1)
    if (maxCursor.hasNext)
      maxCursor.next().as[Long](SEQUENCE_NUMBER)
    else 0L
  }

  private[this] def confirmJournalEntry(pid: String, seq: Long, channelId: String)(implicit ec: ExecutionContext) = Future {
    journal.update(journalEntryQuery(pid, seq), $push(CONFIRMS -> channelId), false, false, writeConcern)
    ()
  }.mapTo[Unit]

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = Future {
    if (to >= from) {
      val cursor = journal.find(journalRangeQuery(pid, from, to)).map(deserializeJournal)

      @tailrec
      def replayLimit(cursor: Iterator[PersistentRepr], remaining: Long): Unit = {
        if (remaining == 0 || !cursor.hasNext) {
          return ()
        }
        replayCallback(cursor.next)
        replayLimit(cursor, remaining - 1)
      }

      replayLimit(cursor, max)
    }
  }

  private[mongodb] def journal(implicit ec: ExecutionContext): MongoCollection = {
    val journalCollection = driver.collection(driver.journalCollectionName)
    journalCollection.ensureIndex(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1, DELETED -> 1),
      MongoDBObject("unique" -> true, "name" -> driver.journalIndexName))
    journalCollection
  }

}