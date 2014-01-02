package akka.contrib.persistence.mongodb

import reactivemongo.bson._
import reactivemongo.bson.buffer.ArrayReadableBuffer
import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.api.indexes.Index
import reactivemongo.api.indexes.IndexType
import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension
import scala.collection.immutable.{Seq => ISeq}
import reactivemongo.core.commands.FindAndModify
import reactivemongo.core.commands.Update
import play.api.libs.iteratee._
import scala.concurrent.Future
import reactivemongo.core.commands.GetLastError
import reactivemongo.core.commands.Aggregate
import reactivemongo.core.commands.Match
import reactivemongo.core.commands.GroupField
import reactivemongo.core.commands.Max
import scala.concurrent.ExecutionContext
import akka.actor.ExtendedActorSystem
import akka.persistence.SelectedSnapshot

object RxMongoPersistenceExtension {
  implicit object BsonBinaryHandler extends BSONHandler[BSONBinary, Array[Byte]] {
    def read(bson: reactivemongo.bson.BSONBinary): Array[Byte] =
      bson.as[Array[Byte]]

    def write(t: Array[Byte]): reactivemongo.bson.BSONBinary =
      BSONBinary(ArrayReadableBuffer(t), Subtype.GenericBinarySubtype)
  }
  
  implicit def futureGleToFutureUnit(gle: Future[GetLastError])(implicit ec: ExecutionContext): Future[Unit] =
    gle.map( _ => () )
}

trait RxMongoPersistenceBase extends MongoPersistenceBase {
  import RxMongoPersistenceExtension._
  
  // Document type
  type D = BSONDocument
  // Collection type
  type C = BSONCollection

  val mongoUrl = settings.Urls
  val mongoDbName = settings.DbName

  val driver = MongoDriver(actorSystem)

  val connection = driver.connection(mongoUrl) 
  
  val db = connection(mongoDbName)(actorSystem.dispatcher)

  val serialization = actorSystem.extension(SerializationExtension)

  private[mongodb] override def collection(name: String)(implicit ec: ExecutionContext): C =
    db[BSONCollection](name)

  implicit object PersistentReprHandler extends BSONDocumentReader[PersistentRepr] with BSONDocumentWriter[PersistentRepr] {
    def read(document: BSONDocument): PersistentRepr = {
      val content = document.getAs[Array[Byte]]("pi").get
      val repr = serialization.deserialize(content, classOf[PersistentRepr]).get
    PersistentRepr(
      repr.payload,
      document.getAs[Long]("sn").get,
      document.getAs[String]("pid").get,
      document.getAs[Boolean]("dl").get,
      repr.resolved,
      repr.redeliveries,
      document.getAs[ISeq[String]]("cs").get,
      repr.confirmable,
      repr.confirmMessage,
      repr.confirmTarget,
      repr.sender)
    }

    def write(persistent: PersistentRepr): BSONDocument = {
      val content = serialization.serialize(persistent).get
      BSONDocument("pid" -> persistent.processorId,
        "sn" -> persistent.sequenceNr,
        "dl" -> persistent.deleted,
        "cs" -> BSONArray(persistent.confirms),
        "pi" -> content)
    }
  }
  
  implicit object SelectedSnapshotHandler extends BSONDocumentReader[SelectedSnapshot] with BSONDocumentWriter[SelectedSnapshot] {
    def read(doc: BSONDocument): SelectedSnapshot = {
      val content = doc.getAs[Array[Byte]]("ss").get
      serialization.deserialize(content, classOf[SelectedSnapshot]).get
    }

    def write(snap: SelectedSnapshot): BSONDocument = {
      val content = serialization.serialize(snap).get
      BSONDocument("pid" -> snap.metadata.processorId,
        "sn" -> snap.metadata.sequenceNr,
        "ts" -> snap.metadata.timestamp,
        "ss" -> content)
    }
  }
}

trait RxMongoPersistenceJournalling extends MongoPersistenceJournalling with RxMongoPersistenceBase {
  private[this] def journalEntryQuery(pid: String, seq: Long) =
    BSONDocument("pid" -> pid, "sn" -> seq)

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) =
    BSONDocument("pid" -> pid, "sn" -> BSONDocument("$gte" -> from, "$lte" -> to))

  private[this] def modifyJournalEntry(query: BSONDocument, op: BSONDocument)(implicit ec: ExecutionContext) =
      journal.db.command(
        new FindAndModify(journal.name, query, Update(op, false))
      )  
  
  private[mongodb] override def journalEntry(pid: String, seq: Long)(implicit ec: ExecutionContext) =
    journal.find(journalEntryQuery(pid,seq)).one[PersistentRepr]

  private[mongodb] override def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) =
  	journal.find(journalRangeQuery(pid,from,to)).cursor[PersistentRepr].collect[Vector]().map(_.iterator)
  	
  private[mongodb] override def appendToJournal(persistent: TraversableOnce[PersistentRepr])(implicit ec: ExecutionContext) =
    Future.fold(persistent.map { doc => journal.insert(doc) } )()( (u,gle) => () )
  
  private[mongodb] override def deleteJournalEntries(pid: String, from: Long, to: Long, permanent: Boolean)(implicit ec: ExecutionContext) = {
    val query = journalRangeQuery(pid, from, to)
    val result = 
      if (permanent) {
	      journal.remove(query)
	    } else {
	      modifyJournalEntry(query,BSONDocument("$set" -> BSONDocument("dl" -> true)))
	    }
    result.map( gle => () )
  }
  
  private[mongodb] def confirmJournalEntry(pid: String, seq: Long, channelId: String)(implicit ec: ExecutionContext) = {
    modifyJournalEntry(journalEntryQuery(pid,seq),BSONDocument("$push" -> BSONDocument("cs" -> channelId))).map(_.getOrElse(BSONDocument()))
  }
  
  private[mongodb] def replayJournal(pid: String, from: Long, to: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = {
      val cursor = journal
        .find(journalRangeQuery(pid, from, to))
        .cursor[PersistentRepr]
      val result: Future[Long] = cursor.enumerate().apply(Iteratee.foreach { p =>
        replayCallback(p)
      }).flatMap(_ => {
        val max = Aggregate(journal.name,
          Seq(Match(BSONDocument("pid" -> pid)),
            GroupField("pid")(("sn", Max("sn")))))
        journal.db.command(max)
      }).map { x => x.headOption.flatMap{ doc => doc.getAs[Long]("sn") }.getOrElse(0) }
      result  
  }
  
  private[mongodb] override def journal(implicit ec: ExecutionContext) = { 
    val journal = db[BSONCollection](settings.JournalCollection)
    journal.indexesManager.ensure(new Index(
      key = Seq(("pid", IndexType.Ascending), ("sn", IndexType.Ascending), ("dl", IndexType.Ascending)),
      background = true,
      unique = true,
      name = Some(settings.JournalIndex)))
    journal
  }

}

trait RxMongoPersistenceSnapshotting extends MongoPersistenceSnapshotting with RxMongoPersistenceBase {
  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = breaker.withCircuitBreaker {
      val selected =
        snaps.find(
          BSONDocument("pid" -> pid,
            "sn" -> BSONDocument("$lte" -> maxSeq),
            "ts" -> BSONDocument("$lte" -> maxTs))).sort(BSONDocument("sn" -> -1, "ts" -> -1))
          .one[SelectedSnapshot]
      selected
    }

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) = 
    breaker.withCircuitBreaker(snaps.insert(snapshot).mapTo[Unit])
  
  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) = breaker.withCircuitBreaker {
    snaps.remove(BSONDocument("pid" -> pid, "sn" -> seq, "ts" -> ts))
  }
  
  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = breaker.withCircuitBreaker {
    snaps.remove(BSONDocument("pid" -> pid, "sn" -> BSONDocument( "$lte" -> maxSeq ), "ts" -> BSONDocument( "$lte" -> maxTs)))
  }
  
  private[mongodb] override def snaps(implicit ec: ExecutionContext) = {
    val snaps = db[BSONCollection](settings.SnapsCollection)
    snaps.indexesManager.ensure(new Index(
      key = Seq(("pid", IndexType.Ascending), ("sq", IndexType.Descending), ("ts", IndexType.Descending)),
      background = true,
      unique = true,
      name = Some(settings.SnapsIndex)))
    snaps
  }
  
}

class RxMongoPersistenceExtension(val actorSystem: ExtendedActorSystem)
	extends MongoPersistenceExtension 
	with RxMongoPersistenceJournalling 
	with RxMongoPersistenceSnapshotting