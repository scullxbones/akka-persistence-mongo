/*
 * Copyright (c) 2013-2018 Brian Scully
 * Copyright (c) 2018      Gael Breard, Orange: Optimization, journal collection cache. PR #181
 * Copyright (c) 2018      Gael Breard, Orange: fix Race condition on deleteFrom. #203
 *
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence._
import com.mongodb.casbah.Imports
import com.mongodb.{DBObject, DuplicateKeyException}
import com.mongodb.casbah.Imports._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class CasbahPersistenceJournaller(val driver: CasbahMongoDriver) extends MongoPersistenceJournallingApi {

  import driver.CasbahSerializers._

  private implicit val system: ActorSystem = driver.actorSystem

  private[this] val logger: Logger = LoggerFactory.getLogger(getClass)

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
    journal.find(journalRangeQuery(pid, from, to))
      .sort(MongoDBObject(TO -> 1))
      .flatMap(_.getAs[MongoDBList](EVENTS))
      .flatMap(lst => lst.collect { case x: DBObject => x })
      .filter(dbo => dbo.getAs[Long](SEQUENCE_NUMBER).exists(sn => sn >= from && sn <= to))
      .map(driver.deserializeJournal)
  }

  private[this] def buildBatch(writes: Seq[AtomicWrite]): Seq[Try[Imports.DBObject]] =
    writes.map(write => Try(driver.serializeJournal(Atom[DBObject](write, driver.useLegacySerialization))))

  private[this] def doBatchAppend(batch: Seq[Try[Imports.DBObject]], collection: MongoCollection)(implicit ec: ExecutionContext): Seq[Try[Imports.DBObject]] = {
    if (batch.forall(_.isSuccess)) {
      val bulk = collection.initializeOrderedBulkOperation
      batch.collect { case scala.util.Success(ser) => ser } foreach bulk.insert
      bulk.execute(writeConcern)
      batch
    } else { // degraded performance, can't batch
      batch.map(_.map { serialized => serialized -> collection.insert(serialized)(identity, writeConcern) })
        .map{_.map{case (ser,_) => ser}}
    }
  }

  private[mongodb] override def batchAppend(writes: Seq[AtomicWrite])(implicit ec: ExecutionContext): Future[Seq[Try[Unit]]] = {
    val batchFuture =
      if (driver.useSuffixedCollectionNames) {
        writes
          .groupBy(write => driver.getJournalCollectionName(write.persistenceId))
          .foldLeft(Future.successful(Seq.empty[Try[Imports.DBObject]])) { case(future, (_, hunk)) =>
            for {
              prev <- future
              batch = buildBatch(hunk)
              next <- Future { doBatchAppend(batch, driver.journal(hunk.head.persistenceId)) }
            } yield prev ++ next
        }
      } else {
        val batch = buildBatch(writes)
        Future { doBatchAppend(batch, journal) }
      }

    if (driver.realtimeEnablePersistence)
      batchFuture.flatMap { batch =>
        val f = Future { doBatchAppend(batch, realtime) }
        f.onFailure {
          case t =>
            logger.error("Error during write to realtime collection", t)
        }
        f.map(squashToUnit)
      }
    else
      batchFuture.map(squashToUnit)
  }

  private[this] def findMaxSequence(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Option[Long] = {
    val $match = MongoDBObject("$match" -> MongoDBObject(PROCESSOR_ID -> persistenceId, TO -> MongoDBObject("$lte" -> maxSequenceNr)))
    val $group = MongoDBObject("$group" -> MongoDBObject("_id" -> s"$$$PROCESSOR_ID", "max" -> MongoDBObject("$max" -> s"$$$TO")))
    val journal = driver.getJournal(persistenceId)
    journal.aggregate($match :: $group :: Nil).results.flatMap(_.getAs[Long]("max")).headOption
  }

  private[this] def setMaxSequenceMetadata(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext) = {
    try {
      metadata.update(
        MongoDBObject(PROCESSOR_ID -> persistenceId),
        $setOnInsert(PROCESSOR_ID -> persistenceId, MAX_SN -> maxSequenceNr),
        upsert = true,
        multi = false,
        concern = driver.metadataWriteConcern
      )
    }
    catch {
      case _:DuplicateKeyException =>
        // Ignore Duplicate Key, PID already exists
    }

    metadata.update(
      MongoDBObject(PROCESSOR_ID -> persistenceId, MAX_SN -> MongoDBObject("$lte" -> maxSequenceNr)),
      $set(MAX_SN -> maxSequenceNr),
      upsert = false,
      multi = false,
      concern = driver.metadataWriteConcern
    )
  }


  private[mongodb] override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = Future {
    val journal = driver.getJournal(persistenceId)


    val maxSn = findMaxSequence(persistenceId, toSequenceNr)
    maxSn.foreach(setMaxSequenceMetadata(persistenceId, _))

    //first remove docs that have to be removed, it avoid settings some docs with from > to and trying to set same from on several docs
    val docWithAllEventsToRemove = (PROCESSOR_ID $eq persistenceId) ++ (TO $lte toSequenceNr)
    journal.remove(docWithAllEventsToRemove)

    //then update the (potential) doc that should have only one (not all) event removed
    //note the query: we exclude documents that have to < toSequenceNr, it should have been deleted just before,
    // but we avoid here some potential race condition that would lead to have from > to and several documents with same from
    val query = journalRangeQuery(persistenceId, toSequenceNr, toSequenceNr)
    val update: DBObject = MongoDBObject(
      "$pull" -> MongoDBObject(
        EVENTS -> MongoDBObject(
          PROCESSOR_ID -> persistenceId,
          SEQUENCE_NUMBER -> MongoDBObject("$lte" -> toSequenceNr))),
      "$set" -> MongoDBObject(FROM -> (toSequenceNr + 1)))
    try {
      journal.update(query, update, upsert = false, multi = true, writeConcern)

    } catch {
      case _: DuplicateKeyException =>
      // it's ok, (and work is done) it can occur only if another thread was doing the same deleteFrom() with same args, and has just done it before this thread
      // (dup key => Same (pid,from,to) => Same targeted "from" in mongo document => it was the same toSequenceNr value)
    }

    if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && journal.count() == 0) {
      journal.dropCollection()
      driver.removeJournalInCache(persistenceId)
    }
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
