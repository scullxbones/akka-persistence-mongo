/*
 * Copyright (c) 2018-2019 Brian Scully
 *
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl._
import com.mongodb.ErrorCategory
import org.mongodb.scala._
import model.Filters._
import model.Updates._
import model.Aggregates._
import model.{Accumulators, BulkWriteOptions, InsertOneModel, UpdateOptions}
import model.Sorts._
import model.Projections._
import org.mongodb.scala.bson.{BsonDocument, BsonValue}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class ScalaDriverPersistenceJournaller(val driver: ScalaMongoDriver) extends MongoPersistenceJournallingApi {

  import driver.ScalaSerializers._
  import RxStreamsInterop._

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  private[this] val writeConcern = driver.journalWriteConcern

  private[this] def journal(implicit ec: ExecutionContext): driver.C = driver.journal.map(_.withWriteConcern(driver.journalWriteConcern))

  private[this] def realtime(implicit ec: ExecutionContext): driver.C = driver.realtime

  private[this] def metadata(implicit ec: ExecutionContext): driver.C = driver.metadata.map(_.withWriteConcern(driver.metadataWriteConcern))

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    and(
      equal(PROCESSOR_ID, pid),
      gte(FROM, from),
      lte(TO, to)
    )

  private[this] implicit val system: ActorSystem = driver.actorSystem
  private[this] implicit val materializer: Materializer = ActorMaterializer()

  private[mongodb] def journalRange(pid: String, from: Long, to: Long, max: Int)(implicit ec: ExecutionContext) = {
    val journal = driver.getJournal(pid)
    val source =
      Source
        .fromFuture(journal)
        .flatMapConcat(
          _.find(journalRangeQuery(pid, from, to))
            .sort(ascending(TO))
            .projection(include(EVENTS))
            .asAkka
            .take(max.toLong)
        )

    val flow = Flow[BsonDocument]
      .mapConcat[Event](e =>
        Option(e.get(EVENTS)).filter(_.isArray).map(_.asArray).map(_.getValues.asScala.toList.collect {
          case d: BsonDocument => driver.deserializeJournal(d)
        }).getOrElse(Seq.empty[Event])
      )
      .filter(_.sn >= from)
      .filter(_.sn <= to)

    source.via(flow)
  }

  private[this] def buildBatch(writes: Seq[AtomicWrite]): Seq[Try[BsonDocument]] =
    writes.map(aw => Try(driver.serializeJournal(Atom[BsonValue](aw, driver.useLegacySerialization))))

  private[this] def doBatchAppend(batch: Seq[Try[BsonDocument]], collection: driver.C)(implicit ec: ExecutionContext): Future[Seq[Try[BsonDocument]]] = {
    if (batch.forall(_.isSuccess)) {
      val collected: Seq[InsertOneModel[driver.D]] = batch.collect { case Success(doc) => InsertOneModel(doc) }
      collection.flatMap(_.withWriteConcern(writeConcern).bulkWrite(collected, new BulkWriteOptions().ordered(true))
        .toFuture()
        .map(_ => batch))
    } else {
      Future.sequence(batch.map {
        case Success(document: BsonDocument) =>
          collection.flatMap(_.withWriteConcern(writeConcern).insertOne(document).toFuture().map(_ => Success(document)))
        case f: Failure[_] =>
          Future.successful(Failure[BsonDocument](f.exception))
      })
    }
  }

  override private[mongodb] def batchAppend(writes: Seq[AtomicWrite])(implicit ec: ExecutionContext): Future[Seq[Try[Unit]]] = {
    val batchFuture = if (driver.useSuffixedCollectionNames) {
      val fZero = Future.successful(Seq.empty[Try[BsonDocument]])

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

  private[this] def setMaxSequenceMetadata(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      md <- metadata
      _  <- md.updateOne(
        equal(PROCESSOR_ID, persistenceId),
        combine(
          setOnInsert(PROCESSOR_ID, persistenceId),
          setOnInsert(MAX_SN, maxSequenceNr)
        ),
        new UpdateOptions().upsert(true)
      ).toFuture()
      _ <- md.updateOne(
        and(equal(PROCESSOR_ID, persistenceId), lte(MAX_SN, maxSequenceNr)),
        set(MAX_SN, maxSequenceNr),
        new UpdateOptions().upsert(false)
      ).toFuture()
    } yield ()
  }

  private[this] def findMaxSequence(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Future[Option[Long]] = {
    def performAggregation(j: MongoCollection[BsonDocument]): Future[Option[Long]] = {
      j.aggregate(
        `match`(and(equal(PROCESSOR_ID,persistenceId), lte(TO, maxSequenceNr))) ::
        group(s"$$$PROCESSOR_ID", Accumulators.max("max", s"$$$TO")) ::
        Nil
      ).toFuture()
      .map(_.headOption)
      .map(_.flatMap(l => Option(l.asDocument().get("max")).filter(_.isInt64).map(_.asInt64).map(_.getValue)))
    }

    for {
      j   <- driver.getJournal(persistenceId)
      rez <- performAggregation(j)
    } yield rez
  }

  override private[mongodb] def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext) = {
    for {
      journal <- driver.getJournal(persistenceId)
      ms <- findMaxSequence(persistenceId, toSequenceNr)
      _ <- ms.fold(Future.successful(()))(setMaxSequenceMetadata(persistenceId, _))


      //first remove docs that have to be removed, it avoid settings some docs with from > to and trying to set same from on several docs
      docWithAllEventsToRemove = and(equal(PROCESSOR_ID, persistenceId), lte(TO, toSequenceNr))
      removed <- journal.deleteMany(docWithAllEventsToRemove).toFuture()

      //then update the (potential) doc that should have only one (not all) event removed
      //note the query: we exclude documents that have to < toSequenceNr, it should have been deleted just before,
      // but we avoid here some potential race condition that would lead to have from > to and several documents with same from
      query = journalRangeQuery(persistenceId, toSequenceNr, toSequenceNr)
      update = combine(
        pull(EVENTS,
            and(
              equal(PROCESSOR_ID, persistenceId),
              lte(SEQUENCE_NUMBER, toSequenceNr)
            )
        ),
        set(FROM, toSequenceNr + 1)
      )

      _ <- journal.withWriteConcern(writeConcern).updateMany(query, update, new UpdateOptions().upsert(false)).toFuture().recover {
        case we : MongoWriteException if we.getError.getCategory == ErrorCategory.DUPLICATE_KEY =>
        // Duplicate key error:
        // it's ok, (and work is done) it can occur only if another thread was doing the same deleteFrom() with same args, and has just done it before this thread
        // (dup key => Same (pid,from,to) => Same targeted "from" in mongo document => it was the same toSequenceNr value)
      }

    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && removed.wasAcknowledged())
        for {
          n <- journal.countDocuments().toFuture()
          if n == 0
          _ <- journal.drop().toFuture().recover{ case _ => Completed() } // ignore errors
          _ = driver.removeJournalInCache(persistenceId)
        } yield ()
      ()
    }
  }

  private[this] def maxSequenceFromMetadata(pid: String)(previous: Option[Long])(implicit ec: ExecutionContext): Future[Option[Long]] = {
    previous.fold(
      metadata.flatMap(_.find(BsonDocument(PROCESSOR_ID -> pid))
        .projection(BsonDocument(MAX_SN -> 1))
        .first()
        .toFutureOption()
        .map(d => d.flatMap(l => Option(l.asDocument().get(MAX_SN)).filter(_.isInt64).map(_.asInt64).map(_.getValue)))))(l => Future.successful(Option(l)))
  }

  override private[mongodb] def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext) = {
    val journal = driver.getJournal(pid)
    journal.flatMap(_.find(BsonDocument(PROCESSOR_ID -> pid))
      .projection(BsonDocument(TO -> 1))
      .sort(BsonDocument(TO -> -1))
      .first()
      .toFutureOption()
      .map(d => d.flatMap(a => Option(a.asDocument().get(TO)).filter(_.isInt64).map(_.asInt64).map(_.getValue)))
      .flatMap(maxSequenceFromMetadata(pid)(_))
      .map(_.getOrElse(0L)))
  }

  override private[mongodb] def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr => Unit)(implicit ec: ExecutionContext) =
    if (max == 0L) Future.successful(())
    else {
      val maxInt = max.toIntWithoutWrapping
      journalRange(pid, from, to, maxInt).map(_.toRepr).runWith(Sink.foreach[PersistentRepr](replayCallback)).map(_ => ())
    }


}
