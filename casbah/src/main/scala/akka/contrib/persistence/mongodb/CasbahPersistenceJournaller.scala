/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.persistence._
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class CasbahPersistenceJournaller(driver: CasbahMongoDriver) extends MongoPersistenceJournallingApi {

  import driver.CasbahSerializers._

  private implicit val system = driver.actorSystem

  private[this] lazy val writeConcern = driver.journalWriteConcern

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long): DBObject =
    (PROCESSOR_ID $eq pid) ++ (FROM $lte to) ++ (TO $gte from)

  private[this] def clearEmptyDocumentsQuery(pid: String): DBObject =
    (PROCESSOR_ID $eq pid) ++ (EVENTS $size 0)

  private[this] def journal(implicit ec: ExecutionContext): MongoCollection = driver.journal

  private[this] def realtime(implicit ec: ExecutionContext) = driver.realtime

  private[this] def metadata(implicit ec: ExecutionContext) = driver.metadata

  private[mongodb] def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext): Iterator[Event] = {
    val journal = driver.getJournal(pid)

    for {
      atom <- journal
        .find(journalRangeQuery(pid, from, to))
        .sort(MongoDBObject(TO -> 1))
      event <- atom.getAs[MongoDBList](EVENTS).map{ xs =>
        xs.collect {
          case x: DBObject => x
        }
      }.getOrElse(Iterator.empty)
      if event.getAs[Long](SEQUENCE_NUMBER).exists(sn => sn >= from && sn <= to)
    } yield {
      driver.deserializeJournal(event, atom.as[Long](TIMESTAMP))
    }
  }

  import collection.immutable.{Seq => ISeq}

  private[this] def doBatchAppend(writes: ISeq[AtomicWrite], collection: MongoCollection)(implicit ec: ExecutionContext): ISeq[Try[Unit]] = {
    val batch = writes.map(write => Try(driver.serializeJournal(Atom[DBObject](write, driver.useLegacySerialization))))

    if (batch.forall(_.isSuccess)) {
      val bulk = collection.initializeOrderedBulkOperation
      batch.collect { case scala.util.Success(ser) => ser } foreach bulk.insert
      bulk.execute(writeConcern)
      batch.map(t => t.map(_ => ()))
    } else { // degraded performance, can't batch
      batch.map(_.map { serialized => collection.insert(serialized)(identity, writeConcern) }.map(_ => ()))
    }
  }

  private[mongodb] override def batchAppend(writes: ISeq[AtomicWrite])(implicit ec: ExecutionContext): Future[ISeq[Try[Unit]]] = {
    val batchFuture = Future {
      if (driver.useSuffixedCollectionNames) {
        writes.groupBy(write => driver.getJournalCollectionName(write.persistenceId)).flatMap {
          case (_, writeSeq) => doBatchAppend(writeSeq, driver.journal(writeSeq.head.persistenceId))
        }.to[collection.immutable.Seq]
      } else {
        doBatchAppend(writes, journal)
      }
    }

    if (driver.realtimeEnablePersistence)
      batchFuture.flatMap { _ =>
        Future {
          doBatchAppend(writes, realtime)
        }
      }
    else
      batchFuture
  }

  private[this] def findMaxSequence(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Option[Long] = {
    val $match = MongoDBObject("$match" -> MongoDBObject(PROCESSOR_ID -> persistenceId, TO -> MongoDBObject("$lte" -> maxSequenceNr)))
    val $group = MongoDBObject("$group" -> MongoDBObject("_id" -> s"$$$PROCESSOR_ID", "max" -> MongoDBObject("$max" -> s"$$$TO")))
    val journal = driver.getJournal(persistenceId)
    journal.aggregate($match :: $group :: Nil).results.flatMap(_.getAs[Long]("max")).headOption
  }

  private[this] def setMaxSequenceMetadata(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext) = {
    metadata.update(
      MongoDBObject(PROCESSOR_ID -> persistenceId, MAX_SN -> MongoDBObject("$lte" -> maxSequenceNr)),
      MongoDBObject(PROCESSOR_ID -> persistenceId, MAX_SN -> maxSequenceNr),
      upsert = true,
      multi = false,
      concern = driver.metadataWriteConcern)
  }

  private[mongodb] override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = Future {
    val journal = driver.getJournal(persistenceId)
    val query = journalRangeQuery(persistenceId, 0L, toSequenceNr)
    val update: DBObject = MongoDBObject(
      "$pull" -> MongoDBObject(
        EVENTS -> MongoDBObject(
          PROCESSOR_ID -> persistenceId,
          SEQUENCE_NUMBER -> MongoDBObject("$lte" -> toSequenceNr))),
      "$set" -> MongoDBObject(FROM -> (toSequenceNr + 1)))
    val maxSn = findMaxSequence(persistenceId, toSequenceNr)
    journal.update(query, update, upsert = false, multi = true, writeConcern)
    maxSn.foreach(setMaxSequenceMetadata(persistenceId, _))
    journal.remove(clearEmptyDocumentsQuery(persistenceId), writeConcern)
    if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && journal.count() == 0)
      journal.dropCollection()
    ()
  }

  private[mongodb] def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long] = Future {
    val journal = driver.getJournal(pid)
    val query = PROCESSOR_ID $eq pid
    val projection = MongoDBObject(TO -> 1)
    val sort = MongoDBObject(TO -> -1)
    val max = journal.find(query, projection).sort(sort).limit(1).toStream.headOption
    val maxDelete = metadata.findOne(query)
    max.flatMap(_.getAs[Long](TO))
      .orElse(maxDelete.flatMap(_.getAs[Long](MAX_SN)))
      .getOrElse(0L)
  }

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = Future {
    @tailrec
    def replayLimit(cursor: Iterator[Event], remaining: Long): Unit = if (remaining > 0 && cursor.hasNext) {
      replayCallback(cursor.next().toRepr)
      replayLimit(cursor, remaining - 1)
    }

    if (to >= from) {
      replayLimit(journalRange(pid, from, to), max)
    }
  }

}
