package akka.contrib.persistence.mongodb

import akka.persistence._
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Success, Try}

class CasbahPersistenceJournaller(driver: CasbahPersistenceDriver) extends MongoPersistenceJournallingApi {

  import CasbahSerializers._
  import JournallingFieldNames._

  implicit val system = driver.actorSystem

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  private[this] def legacyJournalEntryQuery(pid: String, seq: Long) =
    $and(PROCESSOR_ID $eq pid, SEQUENCE_NUMBER $eq seq)

  private[this] def legacyJournalRangeQuery(pid: String, from: Long, to: Long) =
    $and(PROCESSOR_ID $eq pid, $and(SEQUENCE_NUMBER $gte from, SEQUENCE_NUMBER $lte to))

  private[this] def journalEntryQuery(pid: String, seq: Long) =
    journalRangeQuery(pid,seq,seq)

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    ATOM $elemMatch MongoDBObject(PROCESSOR_ID -> pid, FROM -> MongoDBObject("$gte" -> from), TO -> MongoDBObject("$lte" -> to))

  private[mongodb] override def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext) = Future {
    journal.find(journalEntryQuery(pid, seq))
      .flatMap(_.getAs[MongoDBList](ATOM,EVENTS))
      .flatMap(lst => lst.map(_.asInstanceOf[DBObject]))
      .find(dbo => dbo.getAs[Long](SEQUENCE_NUMBER).contains(seq))
      .map(driver.deserialize)
  }

  private[mongodb] override def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) = Future {
    journal.find(journalRangeQuery(pid, from, to))
      .flatMap(_.getAs[MongoDBList](ATOM,EVENTS))
      .flatMap(lst => lst.map(_.asInstanceOf[DBObject]))
      .filter(dbo => dbo.getAs[Long](SEQUENCE_NUMBER).exists(sn => sn >= from && sn <= to))
      .map(driver.deserialize)
  }

  private[mongodb] override def atomicAppend(write: AtomicWrite)(implicit ec: ExecutionContext):Future[Try[Unit]] = Future {
    val serialized = driver.serialize(write)
    journal.insert(serialized)(identity, writeConcern)
    Success(())
  }

  private[mongodb] override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = Future {
    journal.update(
      journalRangeQuery(persistenceId, 0L, toSequenceNr),
      $pull(
        $and(s"$ATOM.$EVENTS.$PROCESSOR_ID" $eq persistenceId,
             s"$ATOM.$EVENTS.$SEQUENCE_NUMBER" $lte toSequenceNr)
      ),
      upsert = false, multi = true, writeConcern)
    journal.remove(s"$ATOM.$EVENTS" $size 0, writeConcern)
  }.mapTo[Unit]

  private[mongodb] def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long] = Future {
    val maxCursor = journal.find(
        s"$ATOM.$PROCESSOR_ID" $eq pid,
        MongoDBObject(s"$ATOM.$TO" -> 1))
      .sort(MongoDBObject(s"$ATOM.$TO" -> -1)).limit(1)
    if (maxCursor.hasNext)
      maxCursor.next().as[Long](s"$ATOM.$TO")
    else 0L
  }

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = Future {
    @tailrec
    def replayLimit(cursor: Iterator[PersistentRepr], remaining: Long): Unit = if (remaining > 0 && cursor.hasNext) {
      replayCallback(cursor.next())
      replayLimit(cursor, remaining - 1)
    }

    if (to >= from) {
      val cursor = journal.find(journalRangeQuery(pid, from, to)).map(driver.deserialize)
      replayLimit(cursor, max)
    }
  }

  private[mongodb] def journal(implicit ec: ExecutionContext): MongoCollection = {
    val journalCollection = driver.collection(driver.journalCollectionName)
    journalCollection.createIndex(
      MongoDBObject(
        s"$ATOM.$PROCESSOR_ID" -> 1,
        s"$ATOM.$FROM" -> 1,
        s"$ATOM.$TO" -> 1
      ),
      MongoDBObject("unique" -> true, "name" -> driver.journalIndexName))
    journalCollection
  }

}