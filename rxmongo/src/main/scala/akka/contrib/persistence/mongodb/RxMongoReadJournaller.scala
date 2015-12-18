package akka.contrib.persistence.mongodb

import akka.actor._
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.stream.scaladsl.Source
import play.api.libs.iteratee.{Enumeratee, Enumerator, Iteratee}
import play.api.libs.streams.Streams
import reactivemongo.api.commands.Command
import reactivemongo.api.{BSONSerializationPack, QueryOpts}
import reactivemongo.bson._

class CurrentAllEvents(val driver: RxMongoDriver){
  import JournallingFieldNames._
  import RxMongoSerializers._
  import driver.actorSystem.dispatcher

  private val opts = QueryOpts().noCursorTimeout

  private val flatten: Enumeratee[BSONDocument,Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      } : _*
    )
  }

  def initial: Source[Event, Unit] = {
    val enumerator = driver.journal
                      .find(BSONDocument())
                      .options(opts)
                      .sort(BSONDocument(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1))
                      .projection(BSONDocument(EVENTS -> 1))
                      .cursor[BSONDocument]()
                      .enumerate()
                      .through(flatten)
    Source(Streams.enumeratorToPublisher(enumerator andThen Enumerator.eof))
  }

}

class CurrentAllPersistenceIds(val driver: RxMongoDriver) {
  import JournallingFieldNames._
  import driver.actorSystem.dispatcher

  private val flatten: Enumeratee[BSONDocument,String] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(doc.getAs[Vector[String]]("values").get : _*)
  }

  def initial = {
    val q = BSONDocument("distinct" -> driver.journalCollectionName, "key" -> PROCESSOR_ID, "query" -> BSONDocument())
    val cmd = Command.run(BSONSerializationPack)
    val enumerator = cmd(driver.db,cmd.rawCommand(q))
      .cursor[BSONDocument]
      .enumerate()
      .through(flatten)
    Source(Streams.enumeratorToPublisher(enumerator andThen Enumerator.eof))
  }
}

class CurrentEventsByPersistenceId(val driver:RxMongoDriver,persistenceId:String,fromSeq:Long,toSeq:Long){
  import JournallingFieldNames._
  import RxMongoSerializers._
  import driver.actorSystem.dispatcher

  private val flatten: Enumeratee[BSONDocument,Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      } : _*
    )
  }

  private val filter = Enumeratee.filter[Event] { e =>
    e.sn >= fromSeq && e.sn <= toSeq
  }

  def initial = {
    val q = BSONDocument(
      PROCESSOR_ID -> persistenceId,
      FROM -> BSONDocument("$gte" -> fromSeq),
      FROM -> BSONDocument("$lte" -> toSeq)
    )
    val enumerator = driver.journal.find(q)
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .enumerate()
      .through(flatten)
      .through(filter)
    Source(Streams.enumeratorToPublisher(enumerator andThen Enumerator.eof))
  }
}

class RxMongoJournalStream(driver: RxMongoDriver) extends JournalStream[Enumerator[BSONDocument]]{
  import RxMongoSerializers._

  implicit val ec = driver.querySideDispatcher

  override def cursor() = driver.realtime.find(BSONDocument.empty).options(QueryOpts().tailable.awaitData).cursor[BSONDocument]().enumerate()

  private val flatten: Enumeratee[BSONDocument,Event] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      } : _*
    )
  }

  override def publishEvents() = {
    val iteratee = Iteratee.foreach[Event](driver.actorSystem.eventStream.publish)
    cursor().through(flatten).run(iteratee)
    ()
  }
}

class RxMongoReadJournaller(driver: RxMongoDriver) extends MongoPersistenceReadJournallingApi{

  val journalStream = {
    val stream = new RxMongoJournalStream(driver)
    stream.publishEvents()
    stream
  }

  override def currentAllEvents: Source[Event, Unit] = new CurrentAllEvents(driver).initial

  override def currentPersistenceIds: Source[String, Unit] = new CurrentAllPersistenceIds(driver).initial

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long): Source[Event, Unit] =
    new CurrentEventsByPersistenceId(driver,persistenceId,fromSeq,toSeq).initial

  override def subscribeJournalEvents(subscriber: ActorRef): Unit = {
    driver.actorSystem.eventStream.subscribe(subscriber, classOf[Event])
    ()
  }
}
