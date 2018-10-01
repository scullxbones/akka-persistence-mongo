/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.persistence.query.{NoOffset, Offset}
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.{Done, NotUsed}
import com.mongodb.casbah.Imports._
import com.mongodb.{BasicDBObjectBuilder, Bytes, DBObject}
import org.bson.types.ObjectId

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal
import scala.{util => su}

case class MultiCursor(driver: CasbahMongoDriver, query: DBObject) {
  import driver.CasbahSerializers._

  private val cursors: List[driver.C#CursorType] =
    driver.getJournalCollections().map(_.find(query))

  def isCompleted: Boolean =
    cursors.forall(_.isEmpty)

  def close(): Unit =
    cursors.foreach(_.close())

  def hasNext: Boolean =
    cursors.exists(_.nonEmpty)

  def next(): Vector[Event] = {
    cursors.foldLeft(Vector.empty[Event]){ case (acc, c) =>
      if (acc.nonEmpty) acc
      else if (!c.hasNext) acc
      else
        Vector(c.next())
          .flatMap(_.getAs[MongoDBList](EVENTS))
          .flatMap(_.collect{ case d:DBObject => d })
          .map(driver.deserializeJournal)
    }
  }

}

class CurrentPersistenceIds(val driver: CasbahMongoDriver) extends SyncActorPublisher[String, Stream[String]] {
  import driver.CasbahSerializers._

  val temporaryCollectionName = s"persistenceids-${System.currentTimeMillis()}-${su.Random.nextInt(1000)}"
  val temporaryCollection: MongoCollection = driver.collection(temporaryCollectionName)

  override protected def initialCursor: Stream[String] = {
    driver.getJournalCollections().toStream
      .flatMap { journal =>
        journal.aggregate(
          MongoDBObject("$project" -> MongoDBObject(PROCESSOR_ID -> 1)) ::
            MongoDBObject("$group" -> MongoDBObject("_id" -> s"$$$PROCESSOR_ID")) ::
            MongoDBObject("$out" -> temporaryCollectionName) ::
            Nil).results
        temporaryCollection.find().toStream
      }
      .flatMap(_.getAs[String]("_id"))
  }

  override protected def next(c: Stream[String], atMost: Long): (Vector[String], Stream[String]) = {
    val (buf, remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[String]): Boolean = {
    c.isEmpty
  }

  override protected def discard(c: Stream[String]): Unit = {
    temporaryCollection.drop()
  }
}

class CurrentAllEvents(val driver: CasbahMongoDriver) extends SyncActorPublisher[Event, MultiCursor] {
  override protected def initialCursor: MultiCursor =
    MultiCursor(driver, MongoDBObject())

  override protected def next(c: MultiCursor, atMost: Long): (Vector[Event], MultiCursor) = {
    var buf = Vector.empty[Event]
    while(c.hasNext && buf.size < atMost.toInt) {
      buf = buf ++ c.next()
    }
    (buf, c)
  }

  override protected def isCompleted(c: MultiCursor): Boolean = c.isCompleted

  override protected def discard(c: MultiCursor): Unit = c.close()
}

class CurrentEventsByPersistenceId(val driver: CasbahMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long)
  extends SyncActorPublisher[Event, Stream[Event]] {
  import driver.CasbahSerializers._

  override protected def initialCursor: Stream[Event] = {
    driver.getJournal(persistenceId)
      .find((PROCESSOR_ID $eq persistenceId) ++ (FROM $lte toSeq) ++ (TO $gte fromSeq))
      .sort(MongoDBObject(TO -> 1))
      .toStream
      .flatMap(_.getAs[MongoDBList](EVENTS))
      .flatMap(lst => lst.collect { case x: DBObject => x })
      .filter(dbo => dbo.getAs[Long](SEQUENCE_NUMBER).exists(sn => sn >= fromSeq && sn <= toSeq))
      .map(driver.deserializeJournal)
  }

  override protected def next(c: Stream[Event], atMost: Long): (Vector[Event], Stream[Event]) = {
    val (buf, remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[Event]): Boolean = c.isEmpty

  override protected def discard(c: Stream[Event]): Unit = ()
}

class CurrentEventsByTagCursorSource(driver: CasbahMongoDriver, tag: String, fromOffset: Offset)
  extends GraphStage[SourceShape[(Event, Offset)]]
    with JournallingFieldNames {

  import driver.CasbahSerializers._

  private val outlet = Outlet[(Event, Offset)]("out")

  private def collectionCursor =
    driver.getJournalCollections().toStream

  private def buildCursor(coll: MongoCollection, query: DBObject) = {
    val cursor = coll.find(query).sort(MongoDBObject(ID -> 1))
    cursor
      .toStream
      .flatMap { rslt =>
        val id = rslt.as[ObjectId](ID)
        rslt.getAs[MongoDBList](EVENTS).map(_.map(ev => id -> ev))
      }
      .flatMap(lst => lst.collect { case (id, x: DBObject) => id -> x })
      .filter { case (_, dbo) => dbo.getAs[MongoDBList](TAGS).exists(xs => xs.contains(tag)) }
      .map { case (id, dbo) => (cursor, ObjectIdOffset(id.toHexString,id.getDate.getTime), driver.deserializeJournal(dbo)) }
  }

  private def translateOffset: Option[DBObject] =
    fromOffset match {
      case NoOffset =>
        None
      case ObjectIdOffset(hexStr, _) =>
        Option(ID $gt new ObjectId(hexStr))
    }

  override val shape: SourceShape[(Event, Offset)] = SourceShape(outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      private val query = translateOffset.foldLeft[MongoDBObject](TAGS $eq tag){ case(tags, offset) => tags ++= offset }
      private var stream =
        su.Try(collectionCursor)
          .recover(recovery).get
          .flatMap(c => Try(buildCursor(c, query)).recover(recovery).get)
      private var currentCursor: Option[MongoCursor] = None

      private def recovery[U]: PartialFunction[Throwable, Stream[U]] = {
        case NonFatal(t) =>
          fail(outlet, t)
          Stream.empty
      }

      override def onPull(): Unit = {
        stream match {
          case (cur,id,el) #:: remaining =>
            currentCursor = Option(cur)
            stream = remaining
            push(outlet, (el, id))
          case _ =>
            completeStage()
        }
      }

      override def postStop(): Unit = {
        currentCursor.foreach(_.close())
      }

      setHandler(outlet, this)
    }
}

case class CasbahRealtimeResource(driver: CasbahMongoDriver, maybeFilter: Option[DBObject]) {
  private implicit val ec: ExecutionContext = driver.querySideDispatcher
  @volatile private var promise = Promise.successful[Option[DBObject]](None)

  private val cursor = {
    def tailing(): MongoCollection = {
      val c = driver.realtime
      c.addOption(Bytes.QUERYOPTION_TAILABLE)
      c.addOption(Bytes.QUERYOPTION_AWAITDATA)
      c
    }

    maybeFilter.fold(tailing().find())(tailing().find(_))
  }

  private def readNext() = {
    promise = Promise[Option[DBObject]]()
    promise.completeWith(Future(
      if (cursor.hasNext) Option(cursor.next())
      else None
    ))
  }

  def next(): Future[Option[DBObject]] = {
    if (promise.isCompleted) readNext()
    promise.future
  }

  def close(): Future[Done] = {
    cursor.close()
    Future.successful(Done)
  }
}

class CasbahMongoJournalStream(val driver: CasbahMongoDriver) extends JournalStream[MongoCollection] {
  import driver.CasbahSerializers._
  private implicit val ec: ExecutionContext = driver.querySideDispatcher

  def cursorSource(maybeFilter: Option[DBObject]): Source[(Event, Offset), NotUsed] = {
    if (driver.realtimeEnablePersistence)
      Source.unfoldResourceAsync[DBObject, CasbahRealtimeResource](
        create = () => Future.successful(CasbahRealtimeResource(driver, maybeFilter)),
        read = _.next(),
        close = _.close()
      )
      .via(killSwitch.flow)
      .mapConcat(unwrapDocument)
    else Source.empty
  }

  private val unwrapDocument: DBObject => List[(Event, Offset)] = {
    case next: DBObject if next.keySet().contains(EVENTS) =>
      val id = next.as[ObjectId](ID)
      next.as[MongoDBList](EVENTS).collect {
        case x: DBObject => driver.deserializeJournal(x) -> ObjectIdOffset(id.toHexString, id.getDate.getTime)
      }.toList
    case _ =>
      Nil
  }

}

class CasbahPersistenceReadJournaller(driver: CasbahMongoDriver) extends MongoPersistenceReadJournallingApi {

  private val journalStreaming = {
    val stream = new CasbahMongoJournalStream(driver)
    driver.actorSystem.registerOnTermination(stream.stopAllStreams())
    stream
  }

  override def currentAllEvents(implicit m: Materializer): Source[Event, NotUsed] =
    Source.fromGraph(new CurrentAllEvents(driver)).mapMaterializedValue(_ => NotUsed)

  override def currentPersistenceIds(implicit m: Materializer): Source[String, NotUsed] =
    Source.fromGraph(new CurrentPersistenceIds(driver)).mapMaterializedValue(_ => NotUsed)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed] =
    Source.fromGraph(new CurrentEventsByPersistenceId(driver, persistenceId, fromSeq, toSeq)).mapMaterializedValue(_ => NotUsed)

  override def currentEventsByTag(tag: String, fromOffset: Offset)(implicit m: Materializer): Source[(Event, Offset), NotUsed] = {
    Source.fromGraph(new CurrentEventsByTagCursorSource(driver, tag, fromOffset))
  }

  override def checkOffsetIsSupported(offset: Offset): Boolean =
    PartialFunction.cond(offset){
      case NoOffset => true
      case ObjectIdOffset(hexStr, _) => ObjectId.isValid(hexStr)
    }

  override def livePersistenceIds(implicit m: Materializer): Source[String, NotUsed] = {
    journalStreaming.cursorSource(None).map{ case(ev, _) => ev.pid }
  }

  override def liveEventsByPersistenceId(persistenceId: String)(implicit m: Materializer): Source[Event, NotUsed] = {
   journalStreaming.cursorSource(
     Option(BasicDBObjectBuilder.start().append(driver.CasbahSerializers.PROCESSOR_ID, persistenceId).get())
   ).map{ case(ev,_) => ev}
  }

  override def liveEvents(implicit m: Materializer): Source[Event, NotUsed] = {
    journalStreaming.cursorSource(None).map{ case(ev, _) => ev}
  }

  override def liveEventsByTag(tag: String, offset: Offset)(implicit m: Materializer, ord: Ordering[Offset]): Source[(Event, Offset), NotUsed] = {
    journalStreaming.cursorSource(
      Option(BasicDBObjectBuilder.start().append(driver.CasbahSerializers.TAGS, tag).get())
    )
  }
}
