/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence._
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson._

import scala.collection.immutable.{Seq => ISeq}
import scala.concurrent._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

class RxMongoJournaller(driver: RxMongoDriver) extends MongoPersistenceJournallingApi {

  import JournallingFieldNames._
  import driver.RxMongoSerializers._

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  private[this] val writeConcern = driver.journalWriteConcern

  private[this] def journal(implicit ec: ExecutionContext) = driver.journal

  private[this] def realtime = driver.realtime

  private[this] def metadata = driver.metadata

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) = BSONDocument(
    PROCESSOR_ID -> pid,
    TO -> BSONDocument("$gte" -> from),
    FROM -> BSONDocument("$lte" -> to))

  private[this] implicit val system: ActorSystem = driver.actorSystem
  private[this] implicit val materializer: Materializer = ActorMaterializer()
  private[mongodb] def journalRange(pid: String, from: Long, to: Long, max: Int)(implicit ec: ExecutionContext) = {   //Enumerator.flatten
    val journal = driver.getJournal(pid)
    val source =
      Source
        .fromFuture(journal)
        .flatMapConcat(
          _.find(journalRangeQuery(pid, from, to))
            .sort(BSONDocument(TO -> 1))
            .projection(BSONDocument(EVENTS -> 1))
            .cursor[BSONDocument]()
            .documentSource(maxDocs = max)
        )

    val flow = Flow[BSONDocument]
      .mapConcat(_.getAs[BSONArray](EVENTS).map(_.values.collect{
        case d: BSONDocument => driver.deserializeJournal(d)
      }).getOrElse(Stream.empty[Event]))
      .filter(_.sn >= from)
      .filter(_.sn <= to )

    source.via(flow)
  }

  private[this] def writeResultToUnit(wr: WriteResult): Try[Unit] = {
    if (wr.ok) Success(())
    else throw new Exception(wr.writeErrors.map(e => s"${e.errmsg} - [${e.code}]").mkString(",")) with NoStackTrace
  }

  private[this] def doBatchAppend(writes: ISeq[AtomicWrite], collection: Future[BSONCollection])(implicit ec: ExecutionContext): Future[ISeq[Try[Unit]]] = {
    val batch = writes.map(aw => Try(driver.serializeJournal(Atom[BSONDocument](aw, driver.useLegacySerialization))))

    if (batch.forall(_.isSuccess)) {
      val collected = batch.toStream.collect { case Success(doc) => doc }
      collection.flatMap(_.bulkInsert(collected, ordered = true, writeConcern).map(_ => batch.map(_.map(_ => ()))))
    } else {
      Future.sequence(batch.map {
        case Success(document: BSONDocument) =>
          collection.flatMap(_.insert(document, writeConcern).map(writeResultToUnit))
        case f: Failure[_] => Future.successful(Failure[Unit](f.exception))
      })
    }
  }

  private[mongodb] override def batchAppend(writes: ISeq[AtomicWrite])(implicit ec: ExecutionContext): Future[ISeq[Try[Unit]]] = {
    val batchFuture = if (driver.useSuffixedCollectionNames) {
      val fZero = Future.successful(ISeq.empty[Try[Unit]])

      // this should guarantee that futures are performed sequentially...
      writes
        .groupBy(write => driver.getJournalCollectionName(write.persistenceId))
        .foldLeft(fZero) { case (future, (_, hunk)) =>
          for {
            prev <- future
            next <- doBatchAppend(hunk, driver.journal(hunk.head.persistenceId))
          } yield prev ++ next
        }

    } else {
      doBatchAppend(writes, journal)
    }

    if (driver.realtimeEnablePersistence)
      batchFuture.andThen { case _ => doBatchAppend(writes, realtime) }
    else
      batchFuture

  }

  private[this] def findMaxSequence(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Future[Option[Long]] = {
    def performAggregation(j: BSONCollection): Future[Option[Long]] = {
      import j.BatchCommands.AggregationFramework.{GroupField, Match, MaxField}

      j.aggregate(firstOperator = Match(BSONDocument(PROCESSOR_ID -> persistenceId, TO -> BSONDocument("$lte" -> maxSequenceNr))),
        otherOperators = GroupField(PROCESSOR_ID)("max" -> MaxField(TO)) :: Nil).map(
          rez => rez.head(BSONDocumentIdentity).flatMap(_.getAs[Long]("max")).headOption)
    }

    for {
      j <- driver.getJournal(persistenceId)
      rez <- performAggregation(j)
    } yield rez
  }

  private[this] def setMaxSequenceMetadata(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      md <- metadata
      _  <- md.update(
        BSONDocument(PROCESSOR_ID -> persistenceId),
        BSONDocument(
          "$setOnInsert" -> BSONDocument(PROCESSOR_ID -> persistenceId, MAX_SN -> maxSequenceNr)
        ),
        writeConcern = driver.metadataWriteConcern,
        upsert = true,
        multi = false
      )
      _  <- md.update(
        BSONDocument(PROCESSOR_ID -> persistenceId, MAX_SN -> BSONDocument("$lte" -> maxSequenceNr)),
        BSONDocument(
          "$set" -> BSONDocument(MAX_SN -> maxSequenceNr)
        ),
        writeConcern = driver.metadataWriteConcern,
        upsert = false,
        multi = false
      )
    } yield ()
  }

  private[mongodb] override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext) = {
    val journal = driver.getJournal(persistenceId)
    val query = journalRangeQuery(persistenceId, 0L, toSequenceNr)
    val update = BSONDocument(
      "$pull" -> BSONDocument(
        EVENTS -> BSONDocument(
          PROCESSOR_ID -> persistenceId,
          SEQUENCE_NUMBER -> BSONDocument("$lte" -> toSequenceNr))),
      "$set" -> BSONDocument(FROM -> (toSequenceNr + 1)))
    val remove = BSONDocument("$and" ->
      BSONArray(
        BSONDocument(PROCESSOR_ID -> persistenceId),
        BSONDocument(EVENTS -> BSONDocument("$size" -> 0))))
    for {
      ms <- findMaxSequence(persistenceId, toSequenceNr)
      j <- journal
      wrUpdate <- j.update(query, update, writeConcern, upsert = false, multi = true)
      if wrUpdate.ok
      _ <- ms.fold(Future.successful(()))(setMaxSequenceMetadata(persistenceId, _))
      wr <- j.remove(remove, writeConcern)
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && wr.ok)
        for {
          n <- j.count()
          if n == 0
          _ <- j.drop(failIfNotFound = false)
        } yield ()
      ()
    }
  }

  private[this] def maxSequenceFromMetadata(pid: String)(previous: Option[Long])(implicit ec: ExecutionContext): Future[Option[Long]] = {
    previous.fold(
      metadata.flatMap(_.find(BSONDocument(PROCESSOR_ID -> pid))
        .projection(BSONDocument(MAX_SN -> 1))
        .cursor[BSONDocument]()
        .headOption
        .map(d => d.flatMap(_.getAs[Long](MAX_SN)))))(l => Future.successful(Option(l)))
  }

  private[mongodb] override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long] = {
    val journal = driver.getJournal(pid)
    journal.flatMap(_.find(BSONDocument(PROCESSOR_ID -> pid))
      .projection(BSONDocument(TO -> 1))
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