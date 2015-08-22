package akka.contrib.persistence.mongodb

import akka.actor.Props
import akka.persistence.query.{EventEnvelope, Hint}
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._

object AllPersistenceIds {
  def props(driver: CasbahMongoDriver): Props = Props(new AllPersistenceIds(driver))
}

class AllPersistenceIds(driver: CasbahMongoDriver) extends BufferingActorPublisher[String] {
  import CasbahSerializers._

  val perFillLimit = driver.settings.ReadJournalPerFillLimit

  private def fillLimit = math.min(perFillLimit,totalDemand.toIntWithoutWrapping)

  override protected def next(offset: Long): (Vector[String],Long) = {
    val vec = driver.journal
      .distinct(PROCESSOR_ID, MongoDBObject())
      .collect { case s:String => s }
      .toVector
      .slice(offset.toIntWithoutWrapping, offset.toIntWithoutWrapping + fillLimit)

    vec -> (offset + vec.size)
  }
}

object AllEvents {
  def props(driver: CasbahMongoDriver): Props = Props(new AllEvents(driver))
}

class AllEvents(driver: CasbahMongoDriver) extends BufferingActorPublisher[EventEnvelope] {
  import CasbahSerializers._

  val perFillLimit = driver.settings.ReadJournalPerFillLimit

  private def fillLimit = math.min(perFillLimit,totalDemand.toIntWithoutWrapping)

  override protected def next(offset: Long): (Vector[EventEnvelope],Long) = {
    val vec = driver.journal
      .find(MongoDBObject())
      .sort(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1))
      .flatMap(_.getAs[MongoDBList](EVENTS))
      .flatMap(lst => lst.collect {case x:DBObject => x} )
      .map(driver.deserializeJournal)
      .zipWithIndex
      .map { case(e,i) => e.toEnvelope(i + offset) }
      .slice(offset.toIntWithoutWrapping, offset.toIntWithoutWrapping + fillLimit)
      .toVector
    vec -> (offset + vec.size)
  }
}

class CasbahPersistenceReadJournaller(driver: CasbahMongoDriver) extends MongoPersistenceReadJournallingApi {
  override def allPersistenceIds(hints: Hint*): Props = AllPersistenceIds.props(driver)

  override def allEvents(hints: Hint*): Props = AllEvents.props(driver)
}
