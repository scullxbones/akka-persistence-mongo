package akka.contrib.persistence.mongodb

import akka.persistence._
import play.api.libs.iteratee.{Iteratee, Enumeratee, Enumerator}
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson._
import DefaultBSONHandlers._

import scala.collection.immutable.{Seq => ISeq}
import scala.concurrent._
import scala.util.{Failure, Try, Success}

class RxMongoJournaller(driver: RxMongoDriver) extends MongoPersistenceJournallingApi {

  import RxMongoSerializers._
  import JournallingFieldNames._

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  private[this] def journal(implicit ec: ExecutionContext) = driver.journal

  private[this] def realtime = driver.realtime

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) = BSONDocument(
    PROCESSOR_ID -> pid,
    FROM -> BSONDocument("$gte" -> from),
    FROM -> BSONDocument("$lte" -> to)
  )

  private[mongodb] def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) = {
    val enum = journal.find(journalRangeQuery(pid, from, to))
                      .sort(BSONDocument(FROM -> 1))
                      .projection(BSONDocument(EVENTS -> 1))
                      .cursor[BSONDocument]()
                      .enumerate()
                      .flatMap(d => Enumerator(
                        d.as[BSONArray](EVENTS).values.collect {
                          case d:BSONDocument => driver.deserializeJournal(d)
                        }.toSeq : _*))
                      .through(Enumeratee.filter[Event](ev => ev.sn >= from && ev.sn <= to))
    enum.run(Iteratee.getChunks[Event])
  }

  private[this] def writeResultToUnit(wr: WriteResult): Try[Unit] = {
    if (wr.ok) Success(())
    else throw wr
  }

  private[mongodb] override def batchAppend(writes: ISeq[AtomicWrite])(implicit ec: ExecutionContext):Future[ISeq[Try[Unit]]] = {
    val batch = writes.toStream.map(aw => Try(driver.serializeJournal(Atom[BSONDocument](aw, driver.useLegacySerialization))))
    Future.sequence(batch.map {
      case Success(document:BSONDocument) =>
        val result =journal.insert(document, writeConcern).map(writeResultToUnit)
        if(driver.realtimeEnablePersistence) realtime.insert(document, writeConcern).map(writeResultToUnit)
        result
      case f:Failure[_] => Future.successful(Failure[Unit](f.exception))
    })
  }

  private[mongodb] override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext) = {
    val query = journalRangeQuery(persistenceId, 0L, toSequenceNr)
    val update = BSONDocument(
      "$pull" -> BSONDocument(
        EVENTS -> BSONDocument(
          PROCESSOR_ID -> persistenceId,
          SEQUENCE_NUMBER -> BSONDocument("$lte" -> toSequenceNr)
        )),
      "$set" -> BSONDocument(FROM -> (toSequenceNr + 1))
    )
    val remove = BSONDocument("$and" ->
      BSONArray(
        BSONDocument(PROCESSOR_ID -> persistenceId),
        BSONDocument(EVENTS -> BSONDocument("$size" -> 0))
      ))
    for {
      wr <- journal.update(query, update, writeConcern, upsert = false, multi = true)
        if wr.ok
      wr <- journal.remove(remove, writeConcern)
    } yield ()
  }

  private[mongodb] override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext) =
    journal.find(BSONDocument(PROCESSOR_ID -> pid))
      .projection(BSONDocument(TO -> 1))
      .sort(BSONDocument(TO -> -1))
      .cursor[BSONDocument]()
      .headOption
      .map(d => d.flatMap(_.getAs[Long](TO)).getOrElse(0L))

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) =
    if (max == 0L) Future.successful(())
    else {
      val maxInt = max.toIntWithoutWrapping
      journalRange(pid, from, to).map(_.take(maxInt).map(_.toRepr).foreach(replayCallback))
    }

}