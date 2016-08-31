/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.persistence._
import org.slf4j.LoggerFactory
import play.api.libs.iteratee.{ Enumeratee, Enumerator, Iteratee }
import reactivemongo.api.Failover2
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson._

import scala.collection.immutable.{ Seq => ISeq }
import scala.concurrent._
import scala.util.control.NoStackTrace
import scala.util.{ Failure, Success, Try }

class RxMongoJournaller(driver: RxMongoDriver) extends MongoPersistenceJournallingApi {

  import JournallingFieldNames._
  import RxMongoSerializers._

  protected val logger = LoggerFactory.getLogger(getClass)

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  private[this] def journal(implicit ec: ExecutionContext) = driver.journal

  private[this] def realtime = driver.realtime

  private[this] def metadata = driver.metadata

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) = BSONDocument(
    PROCESSOR_ID -> pid,
    TO -> BSONDocument("$gte" -> from),
    FROM -> BSONDocument("$lte" -> to))

  private[mongodb] def journalRange(pid: String, from: Long, to: Long, max: Int)(implicit ec: ExecutionContext) = {
    val journal = driver.getJournal(pid)
    journal.find(journalRangeQuery(pid, from, to))
      .sort(BSONDocument(TO -> 1))
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .enumerate(maxDocs = max)
      .flatMap(d => Enumerator(
        d.as[BSONArray](EVENTS).values.collect {
          case d: BSONDocument => driver.deserializeJournal(d)
        }: _*))
      .through(Enumeratee.filter[Event](ev => ev.sn >= from && ev.sn <= to))
  }

  private[this] def writeResultToUnit(wr: WriteResult): Try[Unit] = {
    if (wr.ok) Success(())
    else throw new Exception(wr.errmsg.getOrElse(s"${wr.message} - [${wr.code.fold("N/A")(_.toString)}]")) with NoStackTrace
  }

  /*private[this] def doBatchAppend(writes: ISeq[AtomicWrite], collection: BSONCollection)(implicit ec: ExecutionContext): Future[ISeq[Try[Unit]]] = {
    val batch = writes.map(aw => Try(driver.serializeJournal(Atom[BSONDocument](aw, driver.useLegacySerialization))))

    if (batch.forall(_.isSuccess)) {
      val collected = batch.toStream.collect { case Success(doc) => doc }
      Failover2(driver.connection, driver.failoverStrategy) { () =>
        collection.bulkInsert(collected, ordered = true, writeConcern).map(_ => batch.map(_.map(_ => ())))
      }.future
    } else {
      Future.sequence(batch.map {
        case Success(document: BSONDocument) =>
          collection.insert(document, writeConcern).map(writeResultToUnit)
        case f: Failure[_] => Future.successful(Failure[Unit](f.exception))
      })
    }
  }*/

  private[this] def doBatchAppend(writes: ISeq[AtomicWrite], collection: BSONCollection)(implicit ec: ExecutionContext): Future[ISeq[Try[Unit]]] = {
    val batch = writes.map(aw => Try(driver.serializeJournal(Atom[BSONDocument](aw, driver.useLegacySerialization))))
    val zero = ISeq.empty[Try[Unit]]

    if (batch.forall(_.isSuccess)) {
      val collected = batch.toStream.collect { case Success(doc) => doc }
      Failover2(driver.connection, driver.failoverStrategy) { () =>
          collection.bulkInsert(collected, ordered = true, writeConcern).map(_ => zero).recover {
              case t: Throwable =>
                logger.error(s"Error bulk inserting into ${collection.name} collection", t)
                zero
        }.map(_ => zero)
      }.future
    } else {
      Future.sequence(batch.map {
        case Success(document: BSONDocument) =>
          collection.insert(document, writeConcern).map(_ => zero).recover {
            case t: Throwable =>
              logger.error(s"Error inserting into ${collection.name} collection", t)
              zero
          }.map(_ => Try[Unit](()))
        case f: Failure[_] => Future.successful(Failure[Unit](f.exception))
      })
    }
  }

  private[mongodb] override def batchAppend(writes: ISeq[AtomicWrite])(implicit ec: ExecutionContext): Future[ISeq[Try[Unit]]] = {
    val batchFuture = if (driver.useSuffixedCollectionNames) {
      val fZero = Future[ISeq[Try[Unit]]] { ISeq.empty[Try[Unit]] }

      // this should guarantee that futures are performed sequentially...
      writes.groupBy(_.persistenceId).toList // list of tuples (persistenceId: String, writeSeq: Seq[AtomicWrite])
        .foldLeft(fZero) { (future, tuple) => future.flatMap { _ => doBatchAppend(tuple._2, driver.journal(tuple._1)) } }

    } else {
      doBatchAppend(writes, journal)
    }

    if (driver.realtimeEnablePersistence)
      batchFuture.flatMap { _ => doBatchAppend(writes, realtime) }
    else
      batchFuture

  }

  private[this] def findMaxSequence(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Future[Option[Long]] = {
    val j: BSONCollection = driver.getJournal(persistenceId)
    import j.BatchCommands.AggregationFramework.{ GroupField, Match, Max }

    j.aggregate(firstOperator = Match(BSONDocument(PROCESSOR_ID -> persistenceId, TO -> BSONDocument("$lte" -> maxSequenceNr))),
      otherOperators = GroupField(PROCESSOR_ID)("max" -> Max(TO)) :: Nil).map(
        rez => rez.result.flatMap(_.getAs[Long]("max")).headOption)
  }

  private[this] def setMaxSequenceMetadata(persistenceId: String, maxSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = {
    metadata.update(
      BSONDocument(PROCESSOR_ID -> persistenceId, MAX_SN -> BSONDocument("$lte" -> maxSequenceNr)),
      BSONDocument(PROCESSOR_ID -> persistenceId, MAX_SN -> maxSequenceNr),
      writeConcern = driver.metadataWriteConcern,
      upsert = true,
      multi = false).map(_ => ())
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
      wr <- journal.update(query, update, writeConcern, upsert = false, multi = true)
      if wr.ok
      _ <- ms.fold(Future.successful(()))(setMaxSequenceMetadata(persistenceId, _))
      _ <- journal.remove(remove, writeConcern)
    } yield ()
  }

  private[this] def maxSequenceFromMetadata(pid: String)(previous: Option[Long])(implicit ec: ExecutionContext): Future[Option[Long]] = {
    previous.fold(
      metadata.find(BSONDocument(PROCESSOR_ID -> pid))
        .projection(BSONDocument(MAX_SN -> 1))
        .cursor[BSONDocument]()
        .headOption
        .map(d => d.flatMap(_.getAs[Long](MAX_SN))))(l => Future.successful(Option(l)))
  }

  private[mongodb] override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long] = {
    val journal = driver.getJournal(pid)
    journal.find(BSONDocument(PROCESSOR_ID -> pid))
      .projection(BSONDocument(TO -> 1))
      .sort(BSONDocument(TO -> -1))
      .cursor[BSONDocument]()
      .headOption
      .map(d => d.flatMap(_.getAs[Long](TO)))
      .flatMap(maxSequenceFromMetadata(pid))
      .map(_.getOrElse(0L))
  }

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) =
    if (max == 0L) Future.successful(())
    else {
      val maxInt = max.toIntWithoutWrapping
      (journalRange(pid, from, to, maxInt).map(_.toRepr) through Enumeratee.take(maxInt)).run(Iteratee.foreach {
        replayCallback
      })
    }

}