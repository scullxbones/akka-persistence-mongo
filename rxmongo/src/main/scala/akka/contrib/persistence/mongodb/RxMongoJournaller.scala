/*
 * Copyright (c) 2013-2018 Brian Scully
 * Copyright (c) 2018      Gael Breard, Orange: Optimization, journal collection cache. PR #181
 *
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.slf4j.{Logger, LoggerFactory}
import reactivemongo.akkastream._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.{LastError, WriteResult}
import reactivemongo.bson.{BSONDocument, _}

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

class RxMongoJournaller(val driver: RxMongoDriver) extends MongoPersistenceJournallingApi {

  import JournallingFieldNames._
  import driver.RxMongoSerializers._

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  private[this] def journal(implicit ec: ExecutionContext) = driver.journal

  private[this] def realtime(implicit ec: ExecutionContext) = driver.realtime

  private[this] def metadata(implicit ec: ExecutionContext) = driver.metadata

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) = BSONDocument(
    PROCESSOR_ID -> pid,
    TO -> BSONDocument("$gte" -> from),
    FROM -> BSONDocument("$lte" -> to))

  private[this] implicit val system: ActorSystem = driver.actorSystem
  private[this] implicit val materializer: Materializer = ActorMaterializer()

  private[mongodb] def journalRange(pid: String, from: Long, to: Long, max: Int)(implicit ec: ExecutionContext) = { //Enumerator.flatten
    val journal = driver.getJournal(pid)
    val source =
      Source
        .fromFuture(journal)
        .flatMapConcat(
          _.find(journalRangeQuery(pid, from, to), Option(BSONDocument(EVENTS -> 1)))
            .sort(BSONDocument(TO -> 1))
            .cursor[BSONDocument]()
            .documentSource(maxDocs = max)
        )

    val flow = Flow[BSONDocument]
      .mapConcat(_.getAs[BSONArray](EVENTS).map(_.values.collect {
        case d: BSONDocument => driver.deserializeJournal(d)
      }).getOrElse(Stream.empty[Event]))
      .filter(_.sn >= from)
      .filter(_.sn <= to)

    source.via(flow)
  }

  private[this] def writeResultToUnit(wr: WriteResult, doc: BSONDocument): Try[BSONDocument] = {
    if (wr.ok) Success(doc)
    else throw new Exception(wr.writeErrors.map(e => s"${e.errmsg} - [${e.code}]").mkString(",")) with NoStackTrace
  }

  private[this] def buildBatch(writes: Seq[AtomicWrite]): Seq[Try[BSONDocument]] = {
    writes.map(aw => Try(driver.serializeJournal(Atom[BSONDocument](aw, driver.useLegacySerialization))))
  }

  private[this] def doBatchAppend(batch: Seq[Try[BSONDocument]], collection: Future[BSONCollection])(implicit ec: ExecutionContext): Future[Seq[Try[BSONDocument]]] = {
    if (batch.forall(_.isSuccess)) {
      val collected = batch.toStream.collect { case Success(doc) => doc }
      collection.flatMap(_.insert(ordered = true).many(collected).map(_ => batch))
    } else {
      Future.sequence(batch.map {
        case Success(document: BSONDocument) =>
          collection.flatMap(_.insert(document).map(writeResultToUnit(_, document)))
        case f: Failure[_] => Future.successful(Failure[BSONDocument](f.exception))
      })
    }
  }

  private[mongodb] override def batchAppend(writes: Seq[AtomicWrite])(implicit ec: ExecutionContext): Future[Seq[Try[Unit]]] = {
    val batchFuture = if (driver.useSuffixedCollectionNames) {
      val fZero = Future.successful(Seq.empty[Try[BSONDocument]])

      // this should guarantee that futures are performed sequentially...
      writes
        .groupBy(write => driver.getJournalCollectionName(write.persistenceId))
        .foldLeft(fZero) { case (future, (_, hunk)) =>
          for {
            prev <- future
            batch = buildBatch(hunk)
            next <- doBatchAppend(batch, driver.journal(hunk.head.persistenceId))
          } yield prev ++ next
        }

    } else {
      val batch = buildBatch(writes)
      doBatchAppend(batch, journal)
    }

    if (driver.realtimeEnablePersistence)
      batchFuture.andThen {
        case Success(batch) =>
          val f = doBatchAppend(batch, realtime)
          f.onFailure {
            case t =>
              logger.error("Error during write to realtime collection", t)
          }
          f
      }.map(squashToUnit)
    else
      batchFuture.map(squashToUnit)

  }

  private[this] def findMaxSequence(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Future[Option[Long]] = {
    def performAggregation(j: BSONCollection): Future[Option[Long]] = {
      import j.BatchCommands.AggregationFramework.{GroupField, Match, MaxField}

      j.aggregatorContext[BSONDocument](
        Match(BSONDocument(PROCESSOR_ID -> persistenceId, TO -> BSONDocument("$lte" -> maxSequenceNr))),
        GroupField(PROCESSOR_ID)("max" -> MaxField(TO)) :: Nil,
        batchSize = Option(1)
      ).prepared
        .cursor
        .headOption
        .map(_.flatMap(_.getAs[Long]("max")))
    }

    for {
      j <- driver.getJournal(persistenceId)
      rez <- performAggregation(j)
    } yield rez
  }

  private[this] def setMaxSequenceMetadata(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      md <- metadata
      _ <- md.update(
        BSONDocument(PROCESSOR_ID -> persistenceId),
        BSONDocument(
          "$setOnInsert" -> BSONDocument(PROCESSOR_ID -> persistenceId, MAX_SN -> maxSequenceNr)
        ),
        upsert = true,
        multi = false
      )
      _ <- md.update(
        BSONDocument(PROCESSOR_ID -> persistenceId, MAX_SN -> BSONDocument("$lte" -> maxSequenceNr)),
        BSONDocument(
          "$set" -> BSONDocument(MAX_SN -> maxSequenceNr)
        ),
        upsert = false,
        multi = false
      )
    } yield ()
  }

  private[mongodb] override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext) = {
    for {
      journal <- driver.getJournal(persistenceId)
      ms <- findMaxSequence(persistenceId, toSequenceNr)
      _ <- ms.fold(Future.successful(()))(setMaxSequenceMetadata(persistenceId, _))


      //first remove docs that have to be removed, it avoid settings some docs with from > to and trying to set same from on several docs
      docWithAllEventsToRemove = BSONDocument(PROCESSOR_ID -> persistenceId, TO -> BSONDocument("$lte" -> toSequenceNr))
      removed <- journal.delete().one(docWithAllEventsToRemove)
      if removed.ok


      //then update the (potential) doc that should have only one (not all) event removed
      //note the query: we exclude documents that have to < toSequenceNr, it should have been deleted just before,
      // but we avoid here some potential race condition that would lead to have from > to and several documents with same from
      query = journalRangeQuery(persistenceId, toSequenceNr, toSequenceNr)
      update = BSONDocument(
        "$pull" -> BSONDocument(
          EVENTS -> BSONDocument(
            PROCESSOR_ID -> persistenceId,
            SEQUENCE_NUMBER -> BSONDocument("$lte" -> toSequenceNr))),
        "$set" -> BSONDocument(FROM -> (toSequenceNr + 1)))

      duplicateKeyCodes = Seq(11000,11001,12582)
      _ <- journal.update(query, update, upsert = false, multi = true) recover {
        case le : LastError if le.code.exists(duplicateKeyCodes.contains) || le.writeErrors.exists(we => duplicateKeyCodes.contains(we.code)) =>
        // Duplicate key error:
        // it's ok, (and work is done) it can occur only if another thread was doing the same deleteFrom() with same args, and has just done it before this thread
        // (dup key => Same (pid,from,to) => Same targeted "from" in mongo document => it was the same toSequenceNr value)
      }

    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && removed.ok)
        for {
          n <- journal.count()
          if n == 0
          _ <- journal.drop(failIfNotFound = false)
          _ = driver.removeJournalInCache(persistenceId)
        } yield ()
      ()
    }
  }

  private[this] def maxSequenceFromMetadata(pid: String)(previous: Option[Long])(implicit ec: ExecutionContext): Future[Option[Long]] = {
    previous.fold(
      metadata.flatMap(_.find(BSONDocument(PROCESSOR_ID -> pid), Option(BSONDocument(MAX_SN -> 1)))
        .cursor[BSONDocument]()
        .headOption
        .map(d => d.flatMap(_.getAs[Long](MAX_SN)))))(l => Future.successful(Option(l)))
  }

  private[mongodb] override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long] = {
    val journal = driver.getJournal(pid)
    journal.flatMap(_.find(BSONDocument(PROCESSOR_ID -> pid), Option(BSONDocument(TO -> 1)))
      .sort(BSONDocument(TO -> -1))
      .cursor[BSONDocument]()
      .headOption
      .map(d => d.flatMap(_.getAs[Long](TO)))
      .flatMap(maxSequenceFromMetadata(pid))
      .map(_.getOrElse(0L)))
  }

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) =
    if (max == 0L) Future.successful(())
    else {
      val maxInt = max.toIntWithoutWrapping
      journalRange(pid, from, to, maxInt).map(_.toRepr).runWith(Sink.foreach[PersistentRepr](replayCallback)).map(_ => ())
    }

}