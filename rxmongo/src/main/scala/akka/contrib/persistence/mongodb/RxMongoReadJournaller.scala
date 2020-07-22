/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.persistence.query.{NoOffset, Offset}
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}

import org.reactivestreams.{Publisher, Subscriber, Subscription}
import reactivemongo.akkastream._
import reactivemongo.api.bson._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object CurrentAllEvents {
  def source(driver: RxMongoDriver)(implicit m: Materializer): Source[Event, NotUsed] = {
    import driver.RxMongoSerializers._

    Source.future(driver.journalCollectionsAsFuture)
      .flatMapConcat(_.map { c =>
        c.find(BSONDocument(), Option(BSONDocument(EVENTS -> 1)))
          .cursor[BSONDocument]()
          .documentSource()
          .map { doc =>
            doc.getAsOpt[BSONArray](EVENTS)
              .map(_.values.collect{
                case d:BSONDocument => driver.deserializeJournal(d)
              })
              .getOrElse(Nil)
          }.mapConcat(identity)
      }.reduceLeftOption(_ concat _)
       .getOrElse(Source.empty))
  }
}

object CurrentPersistenceIds {
  def source(driver: RxMongoDriver)(implicit m: Materializer): Source[String, NotUsed] = {
    implicit val ec: ExecutionContext = driver.querySideDispatcher
    val temporaryCollectionName: String = s"persistenceids-${System.currentTimeMillis()}-${Random.nextInt(1000)}"

    Source.future(for {
      collections <- driver.journalCollectionsAsFuture
      tmpNames <- Future.sequence(collections.zipWithIndex.map {
        case (c, idx) =>
          import c.AggregationFramework.{Group, Out, Project}

          val nameWithIndex = s"$temporaryCollectionName-$idx"

          c.aggregatorContext[BSONDocument](
            pipeline = List(
              Project(BSONDocument(PROCESSOR_ID -> 1)),
              Group(BSONString(s"$$$PROCESSOR_ID"))(),
              Out(nameWithIndex)),
            batchSize = Option(1000)
          ).prepared
            .cursor
            .headOption
            .map(_ => nameWithIndex)
      })
      tmps <- Future.sequence(tmpNames.map(driver.collection))
    } yield tmps)
      .flatMapConcat(cols => cols.map(_.find(BSONDocument(), Option.empty[BSONDocument]).cursor[BSONDocument]().documentSource()).reduce(_ ++ _))
      .mapConcat(_.getAsOpt[String]("_id").toList)
      .alsoTo(Sink.onComplete{ _ =>
        driver
          .getCollectionsAsFuture(temporaryCollectionName)
          .foreach(cols =>
            cols.foreach(_.drop(failIfNotFound = false))
          )
      })
  }
}

object CurrentEventsByPersistenceId {
  def queryFor(persistenceId: String, fromSeq: Long, toSeq: Long): BSONDocument = BSONDocument(
    PROCESSOR_ID -> persistenceId,
    TO -> BSONDocument("$gte" -> fromSeq),
    FROM -> BSONDocument("$lte" -> toSeq)
  )

  def source(driver: RxMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed] = {
    import driver.RxMongoSerializers._

    val query = queryFor(persistenceId, fromSeq, toSeq)

    Source.future(driver.getJournal(persistenceId))
            .flatMapConcat(
              _.find(query, Option(BSONDocument(EVENTS -> 1)))
                .sort(BSONDocument(TO -> 1))
                .cursor[BSONDocument]()
                .documentSource()
            ).map( doc =>
              doc.getAsOpt[BSONArray](EVENTS)
                .map(_.values.collect{
                  case d:BSONDocument => driver.deserializeJournal(d)
                })
                .getOrElse(Nil)
            ).mapConcat(identity)
  }
}

object CurrentEventsByTag {
  def source(driver: RxMongoDriver, tag: String, fromOffset: Offset)(implicit m: Materializer): Source[(Event, Offset), NotUsed] = {
    import driver.RxMongoSerializers._

    val offset = fromOffset match {
      case NoOffset => None
      case ObjectIdOffset(hexStr, _) => BSONObjectID.parse(hexStr).toOption
    }
    val query = BSONDocument(
      TAGS -> tag
    ) ++ offset.fold(BSONDocument.empty)(id => BSONDocument(ID -> BSONDocument("$gt" -> id)))

    Source.future(driver.journalCollectionsAsFuture)
          .flatMapConcat{ xs =>
            xs.map(c =>
              c.find(query, Option.empty[BSONDocument])
               .sort(BSONDocument(ID -> 1))
               .cursor[BSONDocument]()
               .documentSource()
            ).reduceLeftOption(_ ++ _)
             .getOrElse(Source.empty)
          }.map{ doc =>
            val id = doc.getAsOpt[BSONObjectID](ID).get
            doc.getAsOpt[BSONArray](EVENTS)
              .map(_.values.collect{
                case d:BSONDocument => driver.deserializeJournal(d) -> ObjectIdOffset(id.stringify, id.time)
              }.filter(_._1.tags.contains(tag)))
              .getOrElse(Nil)
    }.mapConcat(identity)
  }
}

class RxMongoRealtimeGraphStage(driver: RxMongoDriver, bufsz: Int = 16)(factory: Option[BSONObjectID] => Publisher[BSONDocument])
  extends GraphStage[SourceShape[BSONDocument]] {

  private val out = Outlet[BSONDocument]("out")

  override def shape: SourceShape[BSONDocument] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var lastId: Option[BSONObjectID] = None
      private var subscription: Option[Subscription] = None
      private var cursor: Option[Publisher[BSONDocument]] = None
      private var buffer: List[BSONDocument] = Nil

      override def preStart(): Unit = {
        cursor = Option(buildCursor(buildSubscriber()))
      }

      private def subAc = getAsyncCallback[Subscription] { s =>
        s.request(bufsz.toLong)
        subscription = Option(s)
      }

      private def nxtAc = getAsyncCallback[BSONDocument] { doc =>
        if (isAvailable(out)) {
          push(out, doc)
          subscription.foreach(_.request(1L))
        }
        else
          buffer = buffer ::: List(doc)
        lastId = doc.getAsOpt[BSONObjectID]("_id")
      }

      private def errAc = getAsyncCallback[Throwable](failStage)

      private def cmpAc = getAsyncCallback[Unit]{ _ =>
        subscription.foreach(_.cancel())
        cursor = None
        cursor = Option(buildCursor(buildSubscriber()))
      }

      private def buildSubscriber(): Subscriber[BSONDocument] = new Subscriber[BSONDocument] {
        private val subAcImpl = subAc
        private val nxtAcImpl = nxtAc
        private val errAcImpl = errAc
        private val cmpAcImpl = cmpAc

        override def onSubscribe(s: Subscription): Unit = subAcImpl.invoke(s)

        override def onNext(t: BSONDocument): Unit = nxtAcImpl.invoke(t)

        override def onError(t: Throwable): Unit = errAcImpl.invoke(t)

        override def onComplete(): Unit = cmpAcImpl.invoke(())
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          while (buffer.nonEmpty && isAvailable(out)){
            val head :: tail = buffer
            push(out, head)
            buffer = tail
            subscription.foreach(_.request(1L))
          }
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          subscription.foreach(_.cancel())
          completeStage()
        }
      })

      private def buildCursor(subscriber: Subscriber[BSONDocument]): Publisher[BSONDocument] = {
        subscription.foreach(_.cancel())
        val c = factory(lastId)
        c.subscribe(subscriber)
        c
      }
    }
}

class RxMongoJournalStream(driver: RxMongoDriver)(implicit m: Materializer) extends JournalStream[Source[(Event, Offset), NotUsed]] {
  import driver.RxMongoSerializers._

  implicit val ec: ExecutionContext = driver.querySideDispatcher

  def cursor(query: Option[BSONDocument]): Source[(Event, Offset),NotUsed] =
    if (driver.realtimeEnablePersistence)
      Source.future(driver.realtime)
        .flatMapConcat { rt =>
          Source.fromGraph(
            new RxMongoRealtimeGraphStage(driver)(maybeId => {
              ((query, maybeId) match {
                case (None, None) =>
                  rt.find(BSONDocument.empty, Option.empty[BSONDocument])
                case (None, Some(id)) =>
                  rt.find(BSONDocument(ID -> BSONDocument("$gt" -> id)), Option.empty[BSONDocument])
                case (Some(q), None) =>
                  rt.find(q, Option.empty[BSONDocument])
                case (Some(q), Some(id)) =>
                  rt.find(q ++ BSONDocument(ID -> BSONDocument("$gt" -> id)), Option.empty[BSONDocument])
              }).tailable.awaitData.
              cursor[BSONDocument]().documentPublisher()
            })
          )
          .via(killSwitch.flow)
          .mapConcat { d =>
            val id = d.getAsOpt[BSONObjectID](ID).get
            d.getAsOpt[BSONArray](EVENTS).map(_.values.collect {
              case d: BSONDocument => driver.deserializeJournal(d) -> ObjectIdOffset(id.stringify, id.time)
            }).getOrElse(Nil)
          }
        }
    else
      Source.empty
}

class RxMongoReadJournaller(driver: RxMongoDriver)(implicit m: Materializer) extends MongoPersistenceReadJournallingApi {

  val journalStream: RxMongoJournalStream = {
    val stream = new RxMongoJournalStream(driver)(m)
    driver.actorSystem.registerOnTermination(stream.stopAllStreams())
    stream
  }

  override def currentAllEvents(implicit m: Materializer, ec: ExecutionContext): Source[Event, NotUsed] =
    CurrentAllEvents.source(driver)

  override def currentPersistenceIds(implicit m: Materializer, ec: ExecutionContext): Source[String, NotUsed] =
    CurrentPersistenceIds.source(driver)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer, ec: ExecutionContext): Source[Event, NotUsed] =
    CurrentEventsByPersistenceId.source(driver, persistenceId, fromSeq, toSeq)

  override def currentEventsByTag(tag: String, offset: Offset)(implicit m: Materializer, ec: ExecutionContext): Source[(Event, Offset), NotUsed] =
    CurrentEventsByTag.source(driver, tag, offset)

  override def checkOffsetIsSupported(offset: Offset): Boolean =
    PartialFunction.cond(offset){
      case NoOffset => true
      case ObjectIdOffset(hexStr, _) => BSONObjectID.parse(hexStr).isSuccess
    }

  override def liveEventsByPersistenceId(persistenceId: String)(implicit m: Materializer, ec: ExecutionContext): Source[Event, NotUsed] = {
    journalStream.cursor(Option(BSONDocument(
      PROCESSOR_ID -> persistenceId
    ))).mapConcat{ case(ev,_) => List(ev).filter(_.pid == persistenceId) }
  }

  override def liveEvents(implicit m: Materializer, ec: ExecutionContext): Source[Event, NotUsed] = {
    journalStream.cursor(None).map(_._1)
  }

  override def livePersistenceIds(implicit m: Materializer, ec: ExecutionContext): Source[String, NotUsed] = {
    journalStream.cursor(None).map{ case(ev,_) => ev.pid }
  }

  override def liveEventsByTag(tag: String, offset: Offset)(implicit m: Materializer, ec: ExecutionContext, ord: Ordering[Offset]): Source[(Event, Offset), NotUsed] = {
    journalStream.cursor(Option(BSONDocument(
      TAGS -> tag
    ))).filter{ case(ev, off) => ev.tags.contains(tag) &&  ord.gt(off, offset)}
  }
}
