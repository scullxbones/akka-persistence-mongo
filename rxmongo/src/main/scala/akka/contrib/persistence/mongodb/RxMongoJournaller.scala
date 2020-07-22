/*
 * Copyright (c) 2013-2018 Brian Scully
 * Copyright (c) 2018      Gael Breard, Orange: Optimization, journal collection cache. PR #181
 *
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.persistence._
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}
import reactivemongo.akkastream._
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.bson.{BSONDocument, _}
import reactivemongo.api.commands.WriteResult

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

class RxMongoJournaller(val driver: RxMongoDriver) extends MongoPersistenceJournallingApi {

  import JournallingFieldNames._
  import driver.RxMongoSerializers._
  import driver.{materializer, pluginDispatcher}

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  private[this] def journal = driver.journal

  private[this] def realtime = driver.realtime

  private[this] def metadata = driver.metadata

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    BSONDocument(
      PROCESSOR_ID -> pid,
      TO -> BSONDocument(f"$$gte" -> from),
      FROM -> BSONDocument(f"$$lte" -> to)
    )

  private[this] val writeConcern = driver.journalWriteConcern

  def journalRange(pid: String, from: Long, to: Long, max: Int): Source[Event, NotUsed] = { //Enumerator.flatten
    val journal = driver.getJournal(pid)
    val source =
      Source
        .future(journal)
        .flatMapConcat(
          _.find(journalRangeQuery(pid, from, to), Option(BSONDocument(EVENTS -> 1)))
            .sort(BSONDocument(TO -> 1))
            .cursor[BSONDocument]()
            .documentSource(maxDocs = max)
        )

    val flow = Flow[BSONDocument]
      .mapConcat(_.getAsOpt[BSONArray](EVENTS).map(_.values.collect {
        case d: BSONDocument => driver.deserializeJournal(d)
      }).getOrElse(Stream.empty[Event]))
      .filter(_.sn >= from)
      .filter(_.sn <= to)

    source.via(flow)
  }

  private[this] def buildBatch(writes: Seq[AtomicWrite]): Seq[Try[BSONDocument]] = {
    writes.map(aw => Try(driver.serializeJournal(Atom[BSONDocument](aw, driver.useLegacySerialization))))
  }

  private[this] def doBatchAppend(batch: Seq[Try[BSONDocument]], collection: Future[BSONCollection]): Future[Seq[Try[BSONDocument]]] = {
    if (batch.forall(_.isSuccess)) {
      val collected = batch.toStream.collect { case Success(doc) => doc }

      collection.flatMap(
        _.insert(ordered = true).many(collected).map(_ => batch))

    } else {
      Future.sequence(batch.map {
        case Success(document) =>
          collection.flatMap(_.insert(ordered = false, writeConcern).
            one(document).map(_ => Success(document)))

        case Failure(cause) =>
          Future.successful(Failure[BSONDocument](cause))
      })
    }
  }

  override def batchAppend(writes: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
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
          f.onComplete {
            case scala.util.Failure(t) =>
              logger.error("Error during write to realtime collection", t)
            case _ => ()
          }
          f
      }.map(squashToUnit)
    else
      batchFuture.map(squashToUnit)
  }

  private[this] def findMaxSequence(persistenceId: String, maxSequenceNr: Long): Future[Option[Long]] = {
    def performAggregation(j: BSONCollection): Future[Option[Long]] = {
      import j.AggregationFramework.{GroupField, Match, MaxField}

      j.aggregatorContext[BSONDocument](
        pipeline = List(
          Match(BSONDocument(
            PROCESSOR_ID -> persistenceId,
            TO -> BSONDocument(f"$$lte" -> maxSequenceNr))),
          GroupField(PROCESSOR_ID)("max" -> MaxField(TO))),
        batchSize = Option(1)
      ).prepared.cursor.headOption.
        map(_.flatMap(_.getAsOpt[Long]("max")))
    }

    for {
      j <- driver.getJournal(persistenceId)
      rez <- performAggregation(j)
    } yield rez
  }

  private[this] def setMaxSequenceMetadata(persistenceId: String, maxSequenceNr: Long): Future[Unit] = {
    for {
      md <- metadata
      _ <- md.update(ordered = false, writeConcern).one(
            BSONDocument(PROCESSOR_ID -> persistenceId),
            BSONDocument(
              f"$$setOnInsert" -> BSONDocument(
                PROCESSOR_ID -> persistenceId, MAX_SN -> maxSequenceNr)
            ),
            upsert = true,
            multi = false
          )
      _ <- md.update(ordered = false, writeConcern).one(
        BSONDocument(
          PROCESSOR_ID -> persistenceId,
          MAX_SN -> BSONDocument(f"$$lte" -> maxSequenceNr)),
            BSONDocument(
              f"$$set" -> BSONDocument(MAX_SN -> maxSequenceNr)
            ),
            upsert = false,
            multi = false
          )
    } yield ()
  }

  override def deleteFrom(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    for {
      journal <- driver.getJournal(persistenceId)
      ms <- findMaxSequence(persistenceId, toSequenceNr)
      _ <- ms.fold(Future.successful(()))(setMaxSequenceMetadata(persistenceId, _))


      //first remove docs that have to be removed, it avoid settings some docs with from > to and trying to set same from on several docs
      docWithAllEventsToRemove = BSONDocument(
        PROCESSOR_ID -> persistenceId,
        TO -> BSONDocument(f"$$lte" -> toSequenceNr))
      removed <- journal.delete().one(docWithAllEventsToRemove)

      //then update the (potential) doc that should have only one (not all) event removed
      //note the query: we exclude documents that have to < toSequenceNr, it should have been deleted just before,
      // but we avoid here some potential race condition that would lead to have from > to and several documents with same from
      query = journalRangeQuery(persistenceId, toSequenceNr, toSequenceNr)
      update = BSONDocument(
        f"$$pull" -> BSONDocument(
          EVENTS -> BSONDocument(
            PROCESSOR_ID -> persistenceId,
            SEQUENCE_NUMBER -> BSONDocument(f"$$lte" -> toSequenceNr))),
        f"$$set" -> BSONDocument(FROM -> (toSequenceNr + 1)))

      duplicateKeyCodes = Seq(11000, 11001, 12582)
      _ <- journal.update(ordered = false, writeConcern).one(
        query, update,
        upsert = false,
        multi = true).recover {
        // Duplicate key error:
        // it's ok, (and work is done) it can occur only if another thread was doing the same deleteFrom() with same args, and has just done it before this thread
        // (dup key => Same (pid,from,to) => Same targeted "from" in mongo document => it was the same toSequenceNr value)

        case WriteResult.Code(code) if (duplicateKeyCodes contains code) =>
          ()

        case le: WriteResult if (le.writeErrors.exists(
          we => duplicateKeyCodes.contains(we.code))) =>
          ()
      }

    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty)
        driver.removeEmptyJournal(journal)
        .map(_ => driver.removeJournalInCache(persistenceId))
      ()
    }
  }

  private[this] def maxSequenceFromMetadata(pid: String)(previous: Option[Long]): Future[Option[Long]] = {
    previous.fold(
      metadata.flatMap(_.find(BSONDocument(PROCESSOR_ID -> pid), Option(BSONDocument(MAX_SN -> 1)))
        .cursor[BSONDocument]()
        .headOption
        .map(d => d.flatMap(_.getAsOpt[Long](MAX_SN)))))(l => Future.successful(Option(l)))
  }

  override def maxSequenceNr(pid: String, from: Long): Future[Long] = {
    val journal = driver.getJournal(pid)
    journal.flatMap(_.find(BSONDocument(PROCESSOR_ID -> pid), Option(BSONDocument(TO -> 1)))
      .sort(BSONDocument(
        // the PROCESSOR_ID is a workaround for DocumentDB as it would otherwise sort on the compound index due to different optimizations. has no negative effect on MongoDB
        PROCESSOR_ID -> 1,
        TO -> -1
      ))
      .cursor[BSONDocument]()
      .headOption
      .map(d => d.flatMap(_.getAsOpt[Long](TO)))
      .flatMap(maxSequenceFromMetadata(pid))
      .map(_.getOrElse(0L)))
  }

  override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit): Future[Unit] =
    if (max == 0L) Future.successful(())
    else {
      val maxInt = max.toIntWithoutWrapping
      journalRange(pid, from, to, maxInt).map(_.toRepr).runWith(Sink.foreach[PersistentRepr](replayCallback)).map(_ => ())
    }

}
