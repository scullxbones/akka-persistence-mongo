/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.persistence.query.{NoOffset, Offset}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import reactivemongo.akkastream._
import reactivemongo.api.QueryOpts
import reactivemongo.bson._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object CurrentAllEvents {
  def source(driver: RxMongoDriver)(implicit m: Materializer): Source[Event, NotUsed] = {
    import driver.RxMongoSerializers._
    implicit val ec: ExecutionContext = driver.querySideDispatcher

    Source.fromFuture(driver.journalCollectionsAsFuture)
      .flatMapConcat(_.map { c =>
        c.find(BSONDocument())
          .projection(BSONDocument(EVENTS -> 1))
          .cursor[BSONDocument]()
          .documentSource()
          .map { doc =>
            doc.getAs[BSONArray](EVENTS)
              .map(_.elements
                .map(_.value)
                .collect{ case d:BSONDocument => driver.deserializeJournal(d) })
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

    Source.fromFuture(for {
        collections <- driver.journalCollectionsAsFuture
        tmpNames    <- Future.sequence(collections.zipWithIndex.map { case (c,idx) =>
                          import c.BatchCommands.AggregationFramework.{Group, Out, Project}
                          val nameWithIndex = s"$temporaryCollectionName-$idx"
                          c.aggregatorContext[BSONDocument](
                            Project(BSONDocument(PROCESSOR_ID -> 1)),
                            Group(BSONString(s"$$$PROCESSOR_ID"))() ::
                            Out(nameWithIndex) ::
                            Nil,
                            batchSize = Option(1000)
                          ).prepared[AkkaStreamCursor]
                            .cursor
                            .headOption
                            .map(_ => nameWithIndex)
                        })
        tmps         <- Future.sequence(tmpNames.map(driver.collection))
      } yield tmps )
      .flatMapConcat(cols => cols.map(_.find(BSONDocument()).cursor[BSONDocument]().documentSource()).reduce(_ ++ _))
      .mapConcat(_.getAs[String]("_id").toList)
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
  def queryFor(persistenceId: String, fromSeq: Long, toSeq: Long) = BSONDocument(
    PROCESSOR_ID -> persistenceId,
    TO -> BSONDocument("$gte" -> fromSeq),
    FROM -> BSONDocument("$lte" -> toSeq)
  )

  def source(driver: RxMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed] = {
    import driver.RxMongoSerializers._
    implicit val ec: ExecutionContext = driver.querySideDispatcher

    val query = queryFor(persistenceId, fromSeq, toSeq)

    Source.fromFuture(driver.getJournal(persistenceId))
            .flatMapConcat(
              _.find(query)
                .sort(BSONDocument(TO -> 1))
                .projection(BSONDocument(EVENTS -> 1))
                .cursor[BSONDocument]()
                .documentSource()
            ).map( doc =>
              doc.getAs[BSONArray](EVENTS)
                .map(_.elements
                  .map(_.value)
                  .collect{ case d:BSONDocument => driver.deserializeJournal(d) })
                .getOrElse(Nil)
            ).mapConcat(identity)
  }
}

object CurrentEventsByTag {
  def source(driver: RxMongoDriver, tag: String, fromOffset: Offset)(implicit m: Materializer): Source[(Event, Offset), NotUsed] = {
    import driver.RxMongoSerializers._
    implicit val ec: ExecutionContext = driver.querySideDispatcher

    val offset = fromOffset match {
      case NoOffset => None
      case ObjectIdOffset(hexStr, _) => BSONObjectID.parse(hexStr).toOption
    }
    val query = BSONDocument(
      TAGS -> tag
    ).merge(offset.fold(BSONDocument.empty)(id => BSONDocument(ID -> BSONDocument("$gt" -> id))))

    Source.fromFuture(driver.journalCollectionsAsFuture)
          .flatMapConcat{ xs =>
            xs.map(c =>
              c.find(query)
               .sort(BSONDocument(ID -> 1))
               .cursor[BSONDocument]()
               .documentSource()
            ).reduceLeftOption(_ ++ _)
             .getOrElse(Source.empty)
          }.map{ doc =>
            val id = doc.getAs[BSONObjectID](ID).get
            doc.getAs[BSONArray](EVENTS)
              .map(_.elements
                .map(_.value)
                .collect{ case d:BSONDocument => driver.deserializeJournal(d) -> ObjectIdOffset(id.stringify, id.time) }
                .filter(_._1.tags.contains(tag))
              )
              .getOrElse(Nil)
    }.mapConcat(identity)
  }
}

class RxMongoJournalStream(driver: RxMongoDriver)(implicit m: Materializer) extends JournalStream[Source[(Event, Offset), NotUsed]] {
  import driver.RxMongoSerializers._

  implicit val ec: ExecutionContext = driver.querySideDispatcher

  def cursor(query: Option[BSONDocument]): Source[(Event, Offset),NotUsed] =
    if (driver.realtimeEnablePersistence)
      Source.fromFuture(driver.realtime)
        .flatMapConcat { rt =>
          rt.find(query.getOrElse(BSONDocument.empty))
            .options(QueryOpts().tailable.awaitData)
            .cursor[BSONDocument]()
            .documentSource()
            .via(killSwitch.flow)
            .mapConcat { d =>
              val id = d.getAs[BSONObjectID](ID).get
              d.getAs[BSONArray](EVENTS).map(_.elements.map(e => e.value).collect {
                case d: BSONDocument => driver.deserializeJournal(d) -> ObjectIdOffset(id.stringify, id.time)
              }).getOrElse(Nil)
            }
        }
    else
      Source.empty
}

class RxMongoReadJournaller(driver: RxMongoDriver, m: Materializer) extends MongoPersistenceReadJournallingApi {

  val journalStream: RxMongoJournalStream = {
    val stream = new RxMongoJournalStream(driver)(m)
    driver.actorSystem.registerOnTermination(stream.stopAllStreams())
    stream
  }

  override def currentAllEvents(implicit m: Materializer): Source[Event, NotUsed] =
    CurrentAllEvents.source(driver)

  override def currentPersistenceIds(implicit m: Materializer): Source[String, NotUsed] =
    CurrentPersistenceIds.source(driver)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed] =
    CurrentEventsByPersistenceId.source(driver, persistenceId, fromSeq, toSeq)

  override def currentEventsByTag(tag: String, offset: Offset)(implicit m: Materializer): Source[(Event, Offset), NotUsed] =
    CurrentEventsByTag.source(driver, tag, offset)

  override def checkOffsetIsSupported(offset: Offset): Boolean =
    PartialFunction.cond(offset){
      case NoOffset => true
      case ObjectIdOffset(hexStr, _) => BSONObjectID.parse(hexStr).isSuccess
    }

  override def liveEventsByPersistenceId(persistenceId: String)(implicit m: Materializer): Source[Event, NotUsed] = {
    journalStream.cursor(Option(BSONDocument(
      PROCESSOR_ID -> persistenceId
    ))).mapConcat{ case(ev,_) => List(ev).filter(_.pid == persistenceId) }
  }

  override def liveEvents(implicit m: Materializer): Source[Event, NotUsed] = {
    journalStream.cursor(None).map(_._1)
  }

  override def livePersistenceIds(implicit m: Materializer): Source[String, NotUsed] = {
    journalStream.cursor(None).map{ case(ev,_) => ev.pid }
  }

  override def liveEventsByTag(tag: String, offset: Offset)(implicit m: Materializer, ord: Ordering[Offset]): Source[(Event, Offset), NotUsed] = {
    journalStream.cursor(Option(BSONDocument(
      TAGS -> tag
    ))).filter{ case(ev, off) => ev.tags.contains(tag) &&  ord.gt(off, offset)}
  }
}
