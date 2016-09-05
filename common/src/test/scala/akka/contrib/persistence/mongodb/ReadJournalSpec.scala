/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.Props
import akka.persistence.PersistentActor
import akka.persistence.query.{ EventEnvelope, PersistenceQuery }
import akka.stream.scaladsl.{ GraphDSL, Merge, RunnableGraph, Sink }
import akka.stream.{ ActorMaterializer, ClosedShape }
import akka.testkit._
import com.mongodb.client.model.{ BulkWriteOptions, InsertOneModel }
import com.typesafe.config.ConfigFactory
import org.bson.Document
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll }

import scala.concurrent.{ Await, Future, Promise }
import scala.util.Random

abstract class ReadJournalSpec[A <: MongoPersistenceExtension](extensionClass: Class[A], dbName: String, extendedConfig: String = "|") extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll with BeforeAndAfter with Eventually {

  import ConfigLoanFixture._

  override def embedDB = s"read-journal-test-$dbName"

  override def afterAll() = cleanup()

  before {
    val collIterator = mongoClient.getDatabase(embedDB).listCollectionNames().iterator()
    while (collIterator.hasNext()) {
      val name = collIterator.next
      if (name.startsWith("akka_persistence_journal") || name.startsWith("akka_persistence_realtime"))
        mongoClient.getDatabase(embedDB).getCollection(name).drop()
    }
  }

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://$host:$noAuthPort/$embedDB"
    |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
    |akka-contrib-mongodb-persistence-journal {
    |    # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoJournal"
    |}
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka-contrib-mongodb-persistence-snapshot {
    |    # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    |akka-contrib-mongodb-persistence-readjournal {
    |  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoReadJournal"
    |}
    $extendedConfig
    |""".stripMargin).withFallback(ConfigFactory.defaultReference())

  def suffixCollNamesEnabled = config(extensionClass).getValue("akka.contrib.persistence.mongodb.mongo.suffix-builder.class") != null &&
    !config(extensionClass).getValue("akka.contrib.persistence.mongodb.mongo.suffix-builder.class").toString.trim.isEmpty

  def props(id: String, promise: Promise[Unit]) = Props(new Persistent(id, promise))

  case class Append(s: String)

  class Persistent(val persistenceId: String, completed: Promise[Unit]) extends PersistentActor {
    var events = Vector.empty[String]

    override def receiveRecover: Receive = {
      case s: String => events = events :+ s
    }

    override def receiveCommand: Receive = {
      case Append(s) => persist(s) { str =>
        events = events :+ str
        if (str == "END") {
          completed.success(())
          context.stop(self)
        }
      }
    }
  }

  "A read journal" should "support the journal dump query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system = as
      implicit val mat = ActorMaterializer()

      val events = "this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil

      val promise = Promise[Unit]()
      val ar = as.actorOf(props("foo", promise))

      events map Append.apply foreach (ar ! _)

      Await.result(promise.future, 10.seconds.dilated)

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val fut = readJournal.currentAllEvents().runFold(events.toSet) { (received, ee) =>
        val asAppend = ee.event.asInstanceOf[String]
        events should contain(asAppend)
        received - asAppend
      }

      Await.result(fut, 10.seconds.dilated).size shouldBe 0
  }

  it should "support the realtime journal dump query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system = as
      implicit val mat = ActorMaterializer()

      val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

      val promise1 = Promise[Unit]()
      val promise2 = Promise[Unit]()
      val ar1 = as.actorOf(props("foo", promise1))
      val ar2 = as.actorOf(props("bar", promise2))

      events slice (0, 3) foreach (ar1 ! _)

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val probe = TestProbe()

      val promise = Promise[Int]()
      readJournal.allEvents().runFold(0) {
        case (accum, ee) =>
          if (accum == 4) promise.trySuccess(accum)
          probe.ref ! ee
          accum + 1
      }
      events slice (3, 6) foreach (ar2 ! _)

      Await.result(promise.future, 10.seconds.dilated) shouldBe 4

      probe.receiveN(events.size, 10.seconds.dilated).collect { case msg: EventEnvelope => msg.event.toString } should contain allOf ("this", "is", "just", "a", "test", "END")
  }

  it should "support the current persistence ids query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system = as
      implicit val mat = ActorMaterializer()

      val promises = ("1" :: "2" :: "3" :: "4" :: "5" :: Nil).map(id => id -> Promise[Unit]())
      val ars = promises.map { case (id, p) => as.actorOf(props(id, p), s"current-persistenceId-$id") }

      val end = Append("END")
      ars foreach (_ ! end)

      implicit val ec = as.dispatcher
      val futures = promises.map { case (_, p) => p.future }
      val count = Await.result(Future.fold(futures)(0) { case (cnt, _) => cnt + 1 }, 10.seconds.dilated)
      count shouldBe 5

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val fut = readJournal.currentPersistenceIds().runFold(Seq.empty[String])(_ :+ _)

      Await.result(fut, 10.seconds.dilated) should contain allOf ("1", "2", "3", "4", "5")
  }

  it should "support the current persistence ids query with more than 16MB of ids" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      if (!suffixCollNamesEnabled) { // no suffixed collection here, as this test uses a hard coded journal name
        import concurrent.duration._

        implicit val system = as
        implicit val ec = as.dispatcher
        implicit val mat = ActorMaterializer()

        val alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
        val PID_SIZE = 900
        val EVENT_COUNT = 187 * 100

        PID_SIZE * EVENT_COUNT shouldBe >(16 * 1024 * 1024)

        def pidGen = (0 to PID_SIZE).map(_ => alphabet.charAt(Random.nextInt(alphabet.length))).mkString

        import collection.JavaConverters._

        val journalCollection = mongoClient.getDatabase(embedDB).getCollection("akka_persistence_journal")
        Stream.from(1).takeWhile(_ <= EVENT_COUNT).map { i =>
          new Document()
            .append(JournallingFieldNames.PROCESSOR_ID, s"$pidGen-$i")
            .append(JournallingFieldNames.FROM, 0L)
            .append(JournallingFieldNames.TO, 0L)
            .append(JournallingFieldNames.VERSION, 1)
        }.grouped(1000).foreach { dbos =>
          val batch = dbos.toList.map(new InsertOneModel(_)).asJava
          journalCollection.bulkWrite(batch, new BulkWriteOptions().ordered(false).bypassDocumentValidation(true))
          ()
        }

        mongoClient.getDatabase(embedDB).getCollection("akka_persistence_journal").count() shouldBe EVENT_COUNT

        val readJournal =
          PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

        val fut = readJournal.currentPersistenceIds().runFold(0) { case (inc, _) => inc + 1 }
        Await.result(fut, 10.seconds.dilated) shouldBe EVENT_COUNT

        eventually {
          mongoClient.getDatabase(embedDB).listCollectionNames()
            .into(new java.util.HashSet[String]()).asScala.filter(_.startsWith("persistenceids-")) should have size 0L
        }(PatienceConfig(timeout = Span(5L, Seconds), interval = Span(500L, Millis)))
      }
  }

  it should "support the all persistence ids query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system = as
      implicit val mat = ActorMaterializer()

      val promises = ("1" :: "2" :: "3" :: "4" :: "5" :: Nil).map(id => id -> Promise[Unit]())
      val ars = promises.map { case (id, p) => as.actorOf(props(id, p)) }
      val events = ("this" :: "is" :: "a" :: "test" :: Nil) map Append.apply

      implicit val ec = as.dispatcher

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val probe = TestProbe()

      ars slice (0, 3) foreach { ar =>
        events foreach (ar ! _)
      }

      readJournal.allPersistenceIds().runForeach { pid =>
        probe.ref ! pid
      }

      ars slice (3, 5) foreach { ar =>
        events foreach (ar ! _)
      }

      probe.receiveN(ars.size, 10.seconds.dilated).collect { case x: String => x } should contain allOf ("1", "2", "3", "4", "5")
  }

  it should "support the current events by id query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system = as
      implicit val mat = ActorMaterializer()

      val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

      val promise = Promise[Unit]()
      val ar = as.actorOf(props("foo", promise))

      events foreach (ar ! _)

      Await.result(promise.future, 10.seconds.dilated)

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val fut = readJournal.currentEventsByPersistenceId("foo", 0L, 2L).runFold(events.toSet) { (received, ee) =>
        val asAppend = Append(ee.event.asInstanceOf[String])
        events should contain(asAppend)
        received - asAppend
      }

      Await.result(fut, 10.seconds.dilated).map(_.s) shouldBe Set("just", "a", "test", "END")
  }

  it should "support the events by id query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system = as
      implicit val mat = ActorMaterializer()

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val promise = Promise[Unit]()
      val ar = as.actorOf(props("foo-live", promise))

      val events = ("foo" :: "bar" :: "bar2" :: Nil) map Append.apply

      val probe = TestProbe()

      readJournal.eventsByPersistenceId("foo-live", 2L, 3L).runForeach(probe.ref ! _)

      events foreach (ar ! _)

      probe.receiveN(2, 10.seconds.dilated).collect { case msg: EventEnvelope => msg }.toList.map(_.event) should contain allOf ("bar", "bar2")
  }

  it should "support the events by id query with multiple persistent actors" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system = as
      implicit val mat = ActorMaterializer()

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val promise = Promise[Unit]()
      val promise2 = Promise[Unit]()
      val ar = as.actorOf(props("foo-live-2a", promise))
      val ar2 = as.actorOf(props("foo-live-2b", promise2))

      val events = ("foo" :: "bar" :: "bar2" :: Nil) map Append.apply
      val events2 = ("just" :: "a" :: "test" :: Nil) map Append.apply

      val probe = TestProbe()

      readJournal.eventsByPersistenceId("foo-live-2b", 0L, Long.MaxValue).take(events2.size.toLong).runForeach(probe.ref ! _)

      events foreach (ar ! _)
      events2 foreach (ar2 ! _)

      probe.receiveN(events2.size, 10.seconds.dilated).collect { case msg: EventEnvelope => msg }.toList.map(_.event) should be(events2.map(_.s))
  }

  it should "support read 1k events from journal" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system = as
      implicit val mat = ActorMaterializer()

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val nrOfActors = 10
      val nrOfEvents = 100
      val promises = (1 to nrOfActors).map(_ => Promise[Unit]()).zipWithIndex
      val ars = promises map { case (p, idx) => system.actorOf(props(s"pid-${idx + 1}", p), s"actor-${idx + 1}") }
      val events = (1 to nrOfEvents).map(eventId => Append.apply(s"eventd-$eventId")) :+ Append("END")

      val probe = TestProbe()

      val sources = (1 to nrOfActors) map (nr => readJournal.eventsByPersistenceId(s"pid-$nr", 0, Long.MaxValue).take(events.size.toLong))

      val sink = Sink.actorRef(probe.ref, "complete")

      val merged = GraphDSL.create(sink) { implicit b =>
        (s) =>
          import GraphDSL.Implicits._

          val merge = b.add(Merge[EventEnvelope](sources.size))

          sources.foldLeft(0) {
            case (idx, src) =>
              src ~> merge.in(idx)
              idx + 1
          }

          merge.out ~> s

          ClosedShape
      }
      RunnableGraph.fromGraph(merged).run()

      ars foreach { ar =>
        events foreach (ar ! _)
      }

      implicit val ec = as.dispatcher

      val done = Future.sequence(promises.toSeq.map(_._1.future))
      Await.result(done, 1.minute.dilated)

      probe.receiveN(nrOfActors * nrOfEvents, 1.seconds.dilated)
      ()
  }
}
