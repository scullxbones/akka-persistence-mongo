package akka.contrib.persistence.mongodb

import akka.persistence._
import reactivemongo.api.Cursor
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

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) = BSONDocument(
    PROCESSOR_ID -> pid,
    FROM -> BSONDocument("$gte" -> from),
    FROM -> BSONDocument("$lte" -> to)
  )

  private[mongodb] def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) =
    journal.find(journalRangeQuery(pid, from, to))
           .projection(BSONDocument(EVENTS -> 1))
           .cursor[BSONDocument]()
           .foldWhile(ISeq.empty[Event])(unwind(to),{ case(_,thr) => Cursor.Fail(thr)})

  private[this] def unwind(maxSeq: Long)(s: ISeq[Event], doc: BSONDocument) = {
    val docs = doc.as[BSONArray](EVENTS).values.collect {
      case d:BSONDocument => driver.deserializeJournal(d)
    }.filter(_.sn <= maxSeq).sorted
    if (docs.last.sn < maxSeq) Cursor.Cont(s ++ docs)
    else Cursor.Done(s ++ docs)
  }

  private[mongodb] override def atomicAppend(aw: AtomicWrite)(implicit ec: ExecutionContext) = {
    Future(Try(driver.serializeJournal(Atom[BSONDocument](aw)))).flatMap {
      case Success(document:BSONDocument) => journal.insert(document, writeConcern).map(_ => Success(()))
      case f:Failure[_] => Future.successful(Failure[Unit](f.exception))
    }
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