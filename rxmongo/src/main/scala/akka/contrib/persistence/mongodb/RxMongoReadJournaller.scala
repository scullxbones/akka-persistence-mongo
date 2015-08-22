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

class AllEvents(driver: RxMongoDriver) extends NonBlockingBufferingActorPublisher[EventEnvelope] {
  import RxMongoSerializers._
  import JournallingFieldNames._

  val perFillLimit = driver.settings.ReadJournalPerFillLimit

  private def fillLimit = math.min(perFillLimit,totalDemand.toIntWithoutWrapping)

  private def folder(accum: (Vector[EventEnvelope], Long), doc: BSONDocument): Cursor.State[(Vector[EventEnvelope], Long)] = {
    val (vec,offset) = accum
    val envs = doc.as[BSONArray](EVENTS).values.collect {
      case d:BSONDocument => driver.deserializeJournal(d)
    }.zipWithIndex.map{case (ev,idx) => ev.toEnvelope(offset + idx + 1)}
    Cursor.Cont((vec ++ envs, offset + envs.size))
  }

  override protected def next(previousOffset: Long): Future[(Vector[EventEnvelope], Long)] = {
    implicit val ec = context.dispatcher
    driver.journal
      .find(BSONDocument())
      .sort(BSONDocument(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1))
      .options(QueryOpts(skipN = previousOffset.toIntWithoutWrapping))
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .foldWhile(z = (Vector.empty[EventEnvelope],previousOffset), maxDocs = fillLimit)(folder, (_,t) => Cursor.Fail(t))
  }

}

object AllPersistenceIds {
  def props(driver: RxMongoDriver) = Props(new AllPersistenceIds(driver))
}

class AllPersistenceIds(driver: RxMongoDriver) extends NonBlockingBufferingActorPublisher[String] {
  import JournallingFieldNames._

  val perFillLimit = driver.settings.ReadJournalPerFillLimit

  private def fillLimit = math.min(perFillLimit,totalDemand.toIntWithoutWrapping)

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

class RxMongoReadJournaller(driver: RxMongoDriver) extends MongoPersistenceReadJournallingApi {
  override def allPersistenceIds(hints: Hint*): Props = AllPersistenceIds.props(driver)

  override def allEvents(hints: Hint*): Props = AllEvents.props(driver)
}
