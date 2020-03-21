package akka.contrib.persistence.mongodb

import akka.actor.{ActorSystem, Status}
import akka.persistence.query.scaladsl.{CurrentEventsByTagQuery, EventsByTagQuery}
import akka.persistence.query.{EventEnvelope, Offset, PersistenceQuery}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, Materializer}
import akka.testkit._
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters => f}
import com.typesafe.config.{Config, ConfigFactory}
import org.bson.Document
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

abstract class JournalTaggingSpec(extensionClass: Class[_], database: String, extendedConfig: String = "|")
  extends BaseUnitTest
    with ContainerMongo
    with BeforeAndAfterAll
    with Eventually
    with JournallingFieldNames {

  import ConfigLoanFixture._

  override def embedDB = s"tagging-test-$database"

  override def beforeAll(): Unit = cleanup()

  def config(extensionClass: Class[_]): Config =
    ConfigFactory.parseString(s"""|
      |include "/application.conf"
      |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
      |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://$host:$noAuthPort/$embedDB"
      |akka.contrib.persistence.mongodb.mongo.breaker.timeout.call = 0s
      |akka.contrib.persistence.mongodb.mongo.breaker.maxTries = 0
      |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
      |akka-contrib-mongodb-persistence-journal {
      |	  # Class name of the plugin.
      |  class = "akka.contrib.persistence.mongodb.MongoJournal"
      |}
      |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
      |akka-contrib-mongodb-persistence-snapshot {
      |	  # Class name of the plugin.
      |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
      |}
      $extendedConfig
      |""".stripMargin).withFallback(ConfigFactory.defaultReference()).resolve()

  implicit def fn2j8cls[A,B](fn: A => B): com.mongodb.Function[A, B] = {
    new com.mongodb.Function[A, B] {
      def apply(a: A): B = fn(a)
    }
  }

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(1L, Seconds)), interval = scaled(Span(250L, Millis)))

  def eventsFromAtom(d: Document): List[Document] = {
    d.get(EVENTS)
      .asInstanceOf[java.util.List[Document]]
      .asScala
      .toList
  }

  def tagsFromEvent(d: Document): List[String] = {
    Option(d.get(TAGS).asInstanceOf[java.util.List[String]]).map(_.asScala.toList).getOrElse(Nil)
  }

  def tagsFromAtom(d: Document): Set[String] = {
    Option(d.get(TAGS).asInstanceOf[java.util.List[String]]).map(_.asScala.toSet).getOrElse(Set.empty)
  }

  def eventTags(pid: String, sn: Long): Set[String] = {
    val query =
      f.and(f.eq(PROCESSOR_ID, pid), f.lte(FROM, sn), f.gte(TO, sn), f.elemMatch(EVENTS, f.eq(SEQUENCE_NUMBER, sn)))

    whichCollection(pid)
      .find(query)
      .asScala
      .map((d: Document) => eventsFromAtom(d).flatMap(tagsFromEvent).toSet)
      .reduceLeftOption(_ ++ _).getOrElse(Set.empty)
  }

  def atomTags(pid: String, fromSn: Long): Set[String] = {
    val query =
      f.and(f.eq(PROCESSOR_ID, pid), f.eq(FROM, fromSn))

    whichCollection(pid)
      .find(query)
      .asScala
      .map(tagsFromAtom)
      .reduceLeftOption(_ ++ _).getOrElse(Set.empty)
  }

  def whichCollection(pid: String): MongoCollection[Document] = {
    config(extensionClass).withFallback(ConfigFactory.defaultReference()).getString("akka.contrib.persistence.mongodb.mongo.suffix-builder.class") match {
      case "" => akkaPersistenceJournal
      case _  => mongoCollection(SuffixCollectionNamesTest.suffixedCollectionName(pid))
    }
  }

  import TestStubActors._

  private val pids = "foo" :: "bar" :: "baz" :: Nil

  "Several taggers" should "persist tags" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "tagging-test") { case (as, _) =>
    implicit val system: ActorSystem = as
    val respondTo = TestProbe()
    val deathWatch = TestProbe()
    val actorPidPairs = pids.map(pid => pid -> as.actorOf(Taggarific.props(pid),s"tag-$pid"))
    actorPidPairs.foreach{ case(pid,pa) =>
      respondTo.send(pa, Taggarific.TaggedEvent(s"$pid-1", Set()))
      respondTo.expectMsg(Taggarific.Persisted)
    }
    actorPidPairs.foreach{ case(pid,pa) =>
      respondTo.send(pa, Taggarific.TaggedEvent(s"$pid-2", Set("qux", "quux", "quuux")))
      respondTo.expectMsg(Taggarific.Persisted)
    }
    actorPidPairs.foreach{ case(_,pa) =>
      deathWatch.watch(pa)
      pa ! Taggarific.Shutdown
      deathWatch.expectTerminated(pa, 3.seconds.dilated)
    }
    actorPidPairs.foreach{ case(pid, _) =>
      eventually {
        eventTags(pid, 1L) should have size 0
        eventTags(pid, 2L) should contain only("qux", "quux", "quuux")
      }
    }
    actorPidPairs.foreach { case (pid, _) =>
      eventually {
        atomTags(pid, 1L) should have size 0
        atomTags(pid, 2L) should contain only("qux", "quux", "quuux")
      }
    }
  }

  "Query by tag" should "return events that are tagged" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "tagging-test") { case (as, _) =>
    implicit val system: ActorSystem = as
    implicit val am: Materializer = Materializer(as)
    val readJournal = PersistenceQuery(system).readJournalFor[CurrentEventsByTagQuery](MongoReadJournal.Identifier)
    val result = readJournal.currentEventsByTag("qux", Offset.noOffset).runWith(Sink.seq)
    val ees = Await.result(result, 5.seconds.dilated)
    ees.map(_.persistenceId) should contain allOf("foo", "bar", "baz")
    ees.map(_.sequenceNr).distinct should contain only 2L
  }

  "Query by tag" should "not return events that don't match tags" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "tagging-test") { case (as, _) =>
    implicit val system: ActorSystem = as
    implicit val am: Materializer = Materializer(as)
    val readJournal = PersistenceQuery(system).readJournalFor[CurrentEventsByTagQuery](MongoReadJournal.Identifier)
    val result = readJournal.currentEventsByTag("foo", Offset.noOffset).runWith(Sink.seq)
    val ees = Await.result(result, 5.seconds.dilated)
    ees should have size 0
  }

  "Query by tag" should "not allow unsupported offset types" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "tagging-test") { case (as, _) =>
    implicit val system: ActorSystem = as
    implicit val am: Materializer = Materializer(as)
    val readJournal = PersistenceQuery(system).readJournalFor[CurrentEventsByTagQuery](MongoReadJournal.Identifier)
    intercept[IllegalArgumentException]{
      readJournal.currentEventsByTag("foo", Offset.sequence(1L)).runWith(Sink.seq)
      fail("Should have thrown exception for unsupported offset")
    }
  }

  "Current by tag" should "return live events as persisted" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "tagging-test") { case (as, _) =>
    implicit val system: ActorSystem = as
    implicit val am: Materializer = Materializer(as)
    val target = TestProbe()
    val deathWatch = TestProbe()
    val readJournal = PersistenceQuery(system).readJournalFor[EventsByTagQuery](MongoReadJournal.Identifier)
    val killSwitch =
      readJournal
        .eventsByTag("qux", Offset.noOffset)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.actorRef(target.ref, "done", Status.Failure))(Keep.left).run()
    try {
      target.receiveN(3).map(_.asInstanceOf[EventEnvelope].persistenceId) should contain only(pids:_*)
      val newPa = as.actorOf(Taggarific.props("qux"),"tag-qux")
      deathWatch.send(newPa, Taggarific.TaggedEvent("qux", Set("foo","bar","qux")))
      deathWatch.expectMsg(Taggarific.Persisted)
      val liveEvent = target.expectMsgPF(max = 1.seconds.dilated, hint = "Live EventEnvelope"){
        case e:EventEnvelope => e
        case Status.Failure(t) =>
          throw t
        case x =>
          fail(s"Received unexpected message $x")
      }
      liveEvent.persistenceId shouldBe "qux"
      liveEvent.event.asInstanceOf[String] shouldBe "qux"
      deathWatch.watch(newPa)
      deathWatch.send(newPa, Taggarific.Shutdown)
      deathWatch.expectTerminated(newPa, 3.seconds.dilated)
    } finally {
      killSwitch.shutdown()
    }
  }
}
