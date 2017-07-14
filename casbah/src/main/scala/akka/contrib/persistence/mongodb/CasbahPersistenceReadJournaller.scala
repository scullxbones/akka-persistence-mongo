/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.actor.{ActorRef, Props}
import akka.persistence.query.{NoOffset, Offset}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import com.mongodb.casbah.Imports._
import com.mongodb.{Bytes, DBObject}
import org.bson.types.ObjectId

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal
import scala.{util => su}

object CurrentPersistenceIds {
  def props(driver: CasbahMongoDriver): Props = Props(new CurrentPersistenceIds(driver))
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

object CurrentAllEvents {
  def props(driver: CasbahMongoDriver): Props = Props(new CurrentAllEvents(driver))
}

class CurrentAllEvents(val driver: CasbahMongoDriver) extends SyncActorPublisher[Event, Stream[Event]] {
  import driver.CasbahSerializers._

  override protected def initialCursor: Stream[Event] = {
    driver.getJournalCollections().map(_.find(MongoDBObject()).toStream).fold(Stream.empty)(_ ++ _)
      .flatMap(_.getAs[MongoDBList](EVENTS))
      .flatMap(lst => lst.collect { case x: DBObject => x })
      .map(driver.deserializeJournal)
  }

  override protected def next(c: Stream[Event], atMost: Long): (Vector[Event], Stream[Event]) = {
    val (buf, remainder) = c.splitAt(atMost.toIntWithoutWrapping)
    (buf.toVector, remainder)
  }

  override protected def isCompleted(c: Stream[Event]): Boolean = c.isEmpty

  override protected def discard(c: Stream[Event]): Unit = ()
}

object CurrentEventsByPersistenceId {
  def props(driver: CasbahMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long): Props =
    Props(new CurrentEventsByPersistenceId(driver, persistenceId, fromSeq, toSeq))
}

class CurrentEventsByPersistenceId(val driver: CasbahMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long) extends SyncActorPublisher[Event, Stream[Event]] {
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

class CasbahMongoJournalStream(val driver: CasbahMongoDriver) extends JournalStream[MongoCollection] {
  import driver.CasbahSerializers._

  override def cursor(): MongoCollection = {
    val c = driver.realtime
    c.addOption(Bytes.QUERYOPTION_TAILABLE)
    c.addOption(Bytes.QUERYOPTION_AWAITDATA)
    c
  }

  override def publishEvents(): Unit = {
    implicit val ec = driver.querySideDispatcher
    Future {
      cursor().foreach { next =>
        if (next.keySet().contains(EVENTS)) {
          val id = next.as[ObjectId](ID)
          val events = next.as[MongoDBList](EVENTS).collect { case x: DBObject => driver.deserializeJournal(x) -> ObjectIdOffset(id.toHexString, id.getDate.getTime) }
          events.foreach(driver.actorSystem.eventStream.publish)
        }
      }
    }
    ()
  }
}

class CasbahPersistenceReadJournaller(driver: CasbahMongoDriver) extends MongoPersistenceReadJournallingApi {

  private val journalStreaming = {
    val stream = new CasbahMongoJournalStream(driver)
    stream.publishEvents()
    stream
  }

  override def currentAllEvents(implicit m: Materializer): Source[Event, NotUsed] =
    Source.actorPublisher[Event](CurrentAllEvents.props(driver)).mapMaterializedValue(_ => NotUsed)

  override def currentPersistenceIds(implicit m: Materializer): Source[String, NotUsed] =
    Source.actorPublisher[String](CurrentPersistenceIds.props(driver)).mapMaterializedValue(_ => NotUsed)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed] =
    Source.actorPublisher[Event](CurrentEventsByPersistenceId.props(driver, persistenceId, fromSeq, toSeq)).mapMaterializedValue(_ => NotUsed)

  override def currentEventsByTag(tag: String, fromOffset: Offset)(implicit m: Materializer): Source[(Event, Offset), NotUsed] = {
    Source.fromGraph(new CurrentEventsByTagCursorSource(driver, tag, fromOffset))
  }

  override def checkOffsetIsSupported(offset: Offset): Boolean =
    PartialFunction.cond(offset){
      case NoOffset => true
      case ObjectIdOffset(hexStr, _) => ObjectId.isValid(hexStr)
    }

  override def subscribeJournalEvents(subscriber: ActorRef): Unit = {
    driver.actorSystem.eventStream.subscribe(subscriber, classOf[(Event, Offset)])
    ()
  }
}
