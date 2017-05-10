package akka.contrib.persistence.mongodb

import akka.stream.scaladsl._
import org.scalatest.concurrent.ScalaFutures


class RemoveDuplicatedEventsByPersistenceIdSpec extends BaseUnitTest with ScalaFutures with AkkaStreamFixture {

  "RemoveDuplicatedEventsByPersistenceId" should "not remove non duplicate events" in {

    val events = List(
      Event("pid-1", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 3L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 4L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 2L, System.currentTimeMillis(), StringPayload("foo"))
    )

    val processedEvents = Source(events).via(new RemoveDuplicatedEventsByPersistenceId).runFold(Vector.empty[Event])(_ :+ _).futureValue

    processedEvents should contain theSameElementsInOrderAs events
  }

  it should "remove duplicate sequential events" in {

    val events = List(
      Event("pid-1", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 3L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 4L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 2L, System.currentTimeMillis(), StringPayload("foo"))
    )

    val expectedEvents = List(
      Event("pid-1", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 3L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 4L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 2L, System.currentTimeMillis(), StringPayload("foo"))
    )


    val processedEvents = Source(events).via(new RemoveDuplicatedEventsByPersistenceId).runFold(Vector.empty[Event])(_ :+ _).futureValue

    processedEvents should contain theSameElementsInOrderAs expectedEvents
  }

  it should "remove random duplicate events" in {

    val events = List(
      Event("pid-1", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 3L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 4L, System.currentTimeMillis(), StringPayload("foo"))
    )

    val expectedEvents = List(
      Event("pid-1", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 1L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 3L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-2", 2L, System.currentTimeMillis(), StringPayload("foo")),
      Event("pid-1", 4L, System.currentTimeMillis(), StringPayload("foo"))
    )


    val processedEvents = Source(events).via(new RemoveDuplicatedEventsByPersistenceId).runFold(Vector.empty[Event])(_ :+ _).futureValue

    processedEvents should contain theSameElementsInOrderAs expectedEvents
  }


}
