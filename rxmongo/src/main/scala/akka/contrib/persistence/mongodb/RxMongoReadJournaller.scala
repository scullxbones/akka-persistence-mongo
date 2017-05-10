/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.NotUsed
import akka.actor._
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.contrib.persistence.mongodb.RxMongoSerializers.deserializeFlow
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{KillSwitches, Materializer}
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.QueryOpts
import reactivemongo.bson._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object CurrentAllEvents {
  import RxMongoSerializers._

  def source(offset: Long, driver: RxMongoDriver)(implicit m: Materializer): Source[Event, NotUsed] = {
    implicit val ec = driver.querySideDispatcher
    import driver.RxMongoSerializers._

    val query = BSONDocument(TIMESTAMP->BSONDocument("$gte"->BSONLong(offset)))

    Source.fromFuture(driver.journalCollectionsAsFuture)
      .flatMapConcat(_.map { c =>
        c.find(query)
          .projection(BSONDocument(EVENTS -> 1))
          .cursor[BSONDocument]()
          .documentSource()
          .via(deserializeFlow(driver))
      }.reduceLeft(_ concat _))
  }
}

object CurrentAllPersistenceIds {
  def source(driver: RxMongoDriver)(implicit m: Materializer): Source[String, NotUsed] = {
    implicit val ec = driver.querySideDispatcher
    val temporaryCollectionName: String = s"persistenceids-${System.currentTimeMillis()}-${Random.nextInt(1000)}"

    Source.fromFuture(for {
        collections <- driver.journalCollectionsAsFuture
        tmpNames    <- Future.sequence(collections.zipWithIndex.map { case (c,idx) =>
                          import c.BatchCommands.AggregationFramework.{Group, Out, Project}
                          val nameWithIndex = s"$temporaryCollectionName-$idx"
                          c.aggregate(
                            Project(BSONDocument(PROCESSOR_ID -> 1)),
                            Group(BSONString(s"$$$PROCESSOR_ID"))() ::
                            Out(nameWithIndex) ::
                            Nil
                          ).map(_ => nameWithIndex)
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
  def source(driver: RxMongoDriver, persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed] = {
    import driver.RxMongoSerializers._
    implicit val ec = driver.querySideDispatcher

    val query = BSONDocument(
      PROCESSOR_ID -> persistenceId,
      TO -> BSONDocument("$gte" -> fromSeq),
      FROM -> BSONDocument("$lte" -> toSeq))

    Source.fromFuture(driver.getJournal(persistenceId))
            .flatMapConcat(
              _.find(query)
                .sort(BSONDocument(TO -> 1))
                .projection(BSONDocument(EVENTS -> 1))
                .cursor[BSONDocument]()
                .documentSource()
            )
            .via(deserializeFlow(driver))
  }
}

class RxMongoJournalStream(driver: RxMongoDriver)(implicit m: Materializer) extends JournalStream[Source[Event, NotUsed]] {
  import driver.RxMongoSerializers._

  implicit val ec: ExecutionContext = driver.querySideDispatcher

  private val killSwitch = KillSwitches.shared("realtimeKillSwitch")

  override def cursor(): Source[Event,NotUsed] =
    Source.fromFuture(driver.realtime)
      .flatMapConcat {rt =>
        rt.find(BSONDocument.empty)
          .options(QueryOpts().tailable.awaitData)
          .cursor[BSONDocument]()
          .documentSource()
          .via(killSwitch.flow)
          .via(deserializeFlow(driver))
      }

  override def publishEvents(): Unit = {
    val sink = Sink.foreach[Event](driver.actorSystem.eventStream.publish)
    cursor().runWith(sink)
    ()
  }

  def stopAllStreams(): Unit = killSwitch.shutdown()
}

class RxMongoReadJournaller(driver: RxMongoDriver, m: Materializer) extends MongoPersistenceReadJournallingApi {

  val journalStream: RxMongoJournalStream = {
    val stream = new RxMongoJournalStream(driver)(m)
    stream.publishEvents()
    driver.actorSystem.registerOnTermination(stream.stopAllStreams())
    stream
  }

  override def currentAllEvents(offset: Long)(implicit m: Materializer): Source[Event, NotUsed] =
    CurrentAllEvents.source(offset, driver)

  override def currentPersistenceIds(implicit m: Materializer): Source[String, NotUsed] =
    CurrentAllPersistenceIds.source(driver)

  override def currentEventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long)(implicit m: Materializer): Source[Event, NotUsed] =
    CurrentEventsByPersistenceId.source(driver, persistenceId, fromSeq, toSeq)

  override def subscribeJournalEvents(subscriber: ActorRef): Unit = {
    driver.actorSystem.eventStream.subscribe(subscriber, classOf[Event])
    ()
  }
}
