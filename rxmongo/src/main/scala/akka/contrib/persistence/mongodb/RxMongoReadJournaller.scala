package akka.contrib.persistence.mongodb

import akka.actor.Props
import akka.persistence.query.{EventEnvelope, Hint}
import reactivemongo.api.commands.Command
import reactivemongo.api.{BSONSerializationPack, Cursor, QueryOpts}
import reactivemongo.bson._

import scala.concurrent.Future

object AllEvents {
  def props(driver: RxMongoDriver) = Props(new AllEvents(driver))
}

class AllEvents(val driver: RxMongoDriver) extends NonBlockingBufferingActorPublisher[EventEnvelope] {
  import RxMongoSerializers._
  import JournallingFieldNames._

  override protected def next(previousOffset: Long): Future[(Vector[EventEnvelope], Long)] = {
    implicit val ec = context.dispatcher
    driver.journal
      .find(BSONDocument())
      .sort(BSONDocument(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1))
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .foldWhile(z = (Vector.empty[EventEnvelope],0L), maxDocs = fillLimit)(folder(previousOffset), (_,t) => Cursor.Fail(t))
  }

  private def folder(skip: Long)(accum: (Vector[EventEnvelope], Long), doc: BSONDocument): Cursor.State[(Vector[EventEnvelope], Long)] = {
    val (vec,offset) = accum
    val envs = doc.as[BSONArray](EVENTS).values.collect {
      case d:BSONDocument => driver.deserializeJournal(d)
    }.zipWithIndex.map{case (ev,idx) => ev.toEnvelope(offset + idx + 1)}

    val newOffset = offset + envs.size
    if ((envs.size.toLong + offset) > skip) {
      val dropped = envs.drop( (skip - offset).toIntWithoutWrapping )
      if (dropped.nonEmpty) Cursor.Cont((vec ++ dropped, newOffset))
      else Cursor.Done(vec -> newOffset)
    } else Cursor.Cont(vec -> newOffset)
  }
}

object AllPersistenceIds {
  def props(driver: RxMongoDriver) = Props(new AllPersistenceIds(driver))
}

class AllPersistenceIds(val driver: RxMongoDriver) extends NonBlockingBufferingActorPublisher[String] {
  import JournallingFieldNames._

  override protected def next(previousOffset: Long): Future[(Vector[String], Long)] = {
    implicit val ec = context.dispatcher
    val q = BSONDocument("distinct" -> driver.journalCollectionName, "key" -> PROCESSOR_ID, "query" -> BSONDocument())
    val cmd = Command.run(BSONSerializationPack)
    cmd(driver.db,cmd.rawCommand(q))
      .one[BSONDocument]
      .map(_.getAs[Vector[String]]("values").get
      .slice(
        previousOffset.toIntWithoutWrapping,
        previousOffset.toIntWithoutWrapping + fillLimit))
      .map(v => v -> (previousOffset + v.size))
  }
}

object EventsByPersistenceId {
  def props(driver:RxMongoDriver,persistenceId:String,fromSeq:Long,toSeq:Long):Props =
    Props(new EventsByPersistenceId(driver,persistenceId,fromSeq,toSeq))
}

class EventsByPersistenceId(val driver:RxMongoDriver,persistenceId:String,fromSeq:Long,toSeq:Long) extends NonBlockingBufferingActorPublisher[EventEnvelope] {
  import JournallingFieldNames._
  import RxMongoSerializers._

  override protected def next(previousOffset: Long): Future[(Vector[EventEnvelope], Long)] = {
    implicit val ec = context.dispatcher
    val q = BSONDocument(
      PROCESSOR_ID -> persistenceId,
      FROM -> BSONDocument("$gte" -> fromSeq),
      FROM -> BSONDocument("$lte" -> toSeq)
    )
    driver.journal.find(q)
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .foldWhile(z = (Vector.empty[EventEnvelope],0L), maxDocs = fillLimit)(folder(previousOffset), (_,t) => Cursor.Fail(t))
  }

  private def folder(skip: Long)(accum: (Vector[EventEnvelope], Long), doc: BSONDocument): Cursor.State[(Vector[EventEnvelope], Long)] = {
    val (vec,offset) = accum
    val envs = doc.as[BSONArray](EVENTS).values.collect {
      case d:BSONDocument => driver.deserializeJournal(d)
    }.filter(ev => ev.sn <= toSeq && ev.sn >= fromSeq).zipWithIndex.map{case (ev,idx) => ev.toEnvelope(offset + idx + 1)}

    val newOffset = offset + envs.size
    if ((envs.size.toLong + offset) > skip) {
      val dropped = envs.drop( (skip - offset).toIntWithoutWrapping )
      if (dropped.nonEmpty) Cursor.Cont((vec ++ dropped, newOffset))
      else Cursor.Done(vec -> newOffset)
    } else Cursor.Cont(vec -> newOffset)
  }

}

class RxMongoReadJournaller(driver: RxMongoDriver) extends MongoPersistenceReadJournallingApi {
  override def allPersistenceIds(hints: Hint*): Props = AllPersistenceIds.props(driver)

  override def allEvents(hints: Hint*): Props = AllEvents.props(driver)

  override def eventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long, hints: Hint*): Props =
    EventsByPersistenceId.props(driver,persistenceId,fromSeq,toSeq)
}
