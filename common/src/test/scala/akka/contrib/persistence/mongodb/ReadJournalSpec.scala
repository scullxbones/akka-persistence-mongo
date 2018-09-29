/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.{ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.testkit._
import com.mongodb.client.model.{BulkWriteOptions, InsertOneModel}
import com.typesafe.config.{Config, ConfigFactory}
import org.bson.Document
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
import scala.util.Random

abstract class ReadJournalSpec[A <: MongoPersistenceExtension](extensionClass: Class[A], dbName: String, extendedConfig: String = "|")
  extends BaseUnitTest
    with ContainerMongo
    with BeforeAndAfterAll
    with BeforeAndAfter
    with Eventually
    with ScalaFutures {

  import ConfigLoanFixture._

  override def embedDB = s"read-journal-test-$dbName"

  override def afterAll(): Unit = cleanup()

  before {
    val collIterator = mongoClient.getDatabase(embedDB).listCollectionNames().iterator()
    while (collIterator.hasNext) {
      val name = collIterator.next
      if (name.startsWith("akka_persistence_journal") || name.startsWith("akka_persistence_realtime"))
        mongoClient.getDatabase(embedDB).getCollection(name).drop()
    }
  }

  def config(extensionClass: Class[_]): Config = ConfigFactory.parseString(s"""
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

  def suffixCollNamesEnabled: Boolean = config(extensionClass).getString("akka.contrib.persistence.mongodb.mongo.suffix-builder.class") != null &&
    !config(extensionClass).getString("akka.contrib.persistence.mongodb.mongo.suffix-builder.class").trim.isEmpty

  def props(id: String, promise: Promise[Unit], eventCount: Int) = Props(new PersistentCountdown(id, promise, eventCount))

  case class Append(s: String)
  case object RUAlive
  case object IMAlive

  class PersistentCountdown(val persistenceId: String, completed: Promise[Unit], count: Int) extends PersistentActor {
    private var remaining = count

    override def receiveRecover: Receive = {
      case _: String => remaining -= 1
    }

    override def receiveCommand: Receive = {
      case RUAlive =>
        sender() ! IMAlive
      case Append(s) => persist(s) { _ =>
        remaining -= 1
        if (remaining == 0) {
          completed.success(())
          context.stop(self)
        }
      }
    }
  }

  "A read journal" should "support the journal dump query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val mat: ActorMaterializer = ActorMaterializer()

      val events = "this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil

      val promise = Promise[Unit]()
      val ar = as.actorOf(props("foo", promise, 6))

      events map Append.apply foreach (ar ! _)

      Await.result(promise.future, 3.seconds.dilated)

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val fut = readJournal.currentAllEvents().runFold(events.toSet) { (received, ee) =>
        val asAppend = ee.event.asInstanceOf[String]
        events should contain(asAppend)
        received - asAppend
      }

      Await.result(fut, 3.seconds.dilated).size shouldBe 0
  }

  it should "support the realtime journal dump query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val am: ActorMaterializer = ActorMaterializer()

      val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

      val promise1 = Promise[Unit]()
      val promise2 = Promise[Unit]()
      val ar1 = as.actorOf(props("foo", promise1, 3))
      val ar2 = as.actorOf(props("bar", promise2, 3))

      events slice (0, 3) foreach (ar1 ! _)
      Await.ready(promise1.future, 3.seconds)
      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val probe = TestProbe()

      val ks =
        readJournal.allEvents()
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.actorRef[EventEnvelope](probe.ref, 'complete))(Keep.left)
          .run()

      events slice (3, 6) foreach (ar2 ! _)
      implicit val ec: ExecutionContextExecutor = as.dispatcher

      probe.receiveN(events.size, 3.seconds.dilated).collect { case msg: EventEnvelope => msg.event.toString } should contain allOf ("this", "is", "just", "a", "test", "END")
      ks.shutdown()
  }

  it should "support the current all events query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val mat: ActorMaterializer = ActorMaterializer()

      val promises = ("1" :: "2" :: "3" :: "4" :: "5" :: Nil).map(id => id -> Promise[Unit]())
      
      val events = "this" :: "is" :: "just" :: "a" :: "test" :: Nil      
      val allEvents = promises.map {case (id, _) => id -> events.map(event => s"$event$id")}
      
      allEvents foreach {case (id, evs) => 
        promises collect {
          case (i,p) if i == id =>
            val ar = as.actorOf(props(i, p, 5), s"current-all-events-$i")
            evs map Append.apply foreach (ar ! _)
            ar ! Append("END")
        }
      }

      implicit val ec: ExecutionContextExecutor = as.dispatcher
      val futures = promises.map { case (_, p) => p.future }
      val count = Await.result(Future.fold(futures)(0) { case (cnt, _) => cnt + 1 }, 10.seconds.dilated)
      count shouldBe 5

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
        
      val expectedEvents = allEvents flatMap {case (_, evs) => evs}

      val fut = readJournal.currentAllEvents().runFold(expectedEvents.toSet) { (received, ee) =>
        val asAppend = ee.event.asInstanceOf[String]
        if (!asAppend.equals("END"))
          expectedEvents should contain(asAppend)
        received - asAppend
      }

      Await.result(fut, 15.seconds.dilated).size shouldBe 0
  }

  it should "support the current persistence ids query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val mat: ActorMaterializer = ActorMaterializer()
      
      implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = 5.seconds.dilated, interval = 500.millis.dilated)

      val promises = ("1" :: "2" :: "3" :: "4" :: "5" :: Nil).map(id => id -> Promise[Unit]())
      val ars = promises.map { case (id, p) => as.actorOf(props(id, p, 1), s"current-persistenceId-$id") }

      val end = Append("END")
      ars foreach (_ ! end)

      implicit val ec: ExecutionContextExecutor = as.dispatcher
      val waitForStop = Future.sequence(promises.map { case (_, p) => p.future })
      Await.result(waitForStop, 3.seconds.dilated) should have size 5

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val probe = TestProbe()
      readJournal.currentPersistenceIds().runWith(Sink.actorRef(probe.ref, 'complete))

      probe.receiveN(6, 5.seconds.dilated) should contain allOf ("1", "2", "3", "4", "5")

      eventually {
        mongoClient.getDatabase(embedDB).listCollectionNames()
          .into(new java.util.HashSet[String]()).asScala.filter(_.startsWith("persistenceids-")) should have size 0L
      }
  }

  it should "support the current persistence ids query with more than 16MB of ids" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      assume(!suffixCollNamesEnabled) // no suffixed collection here, as this test uses a hard coded journal name
      import concurrent.duration._

      implicit val system: ActorSystem = as

      implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = 5.seconds.dilated, interval = 500.millis.dilated)

      implicit val ec: ExecutionContextExecutor = as.dispatcher
      implicit val mat: ActorMaterializer = ActorMaterializer()

      val alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
      val PID_SIZE = 900
      val EVENT_COUNT = 187 * 100

      PID_SIZE * EVENT_COUNT shouldBe >(16 * 1024 * 1024)

      def pidGen = (0 to PID_SIZE).map(_ => alphabet.charAt(Random.nextInt(alphabet.length))).mkString

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
      Await.result(fut, 5.seconds.dilated) shouldBe EVENT_COUNT

      eventually {
        mongoClient.getDatabase(embedDB).listCollectionNames()
          .into(new java.util.HashSet[String]()).asScala.filter(_.startsWith("persistenceids-")) should have size 0L
      }
  }

  it should "support the all persistence ids query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val mat: ActorMaterializer = ActorMaterializer()
      
      implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = 5.seconds.dilated, interval = 500.millis.dilated)

      val promises = ("1" :: "2" :: "3" :: "4" :: "5" :: Nil).map(id => id -> Promise[Unit]())
      val ars = promises.map { case (id, p) => as.actorOf(props(id, p, 4)) }
      val events = ("this" :: "is" :: "a" :: "test" :: Nil) map Append.apply

      implicit val ec: ExecutionContextExecutor = as.dispatcher

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val probe = TestProbe()

      ars.slice(0,3).foreach(ar =>events.foreach(ar ! _))

      Await.ready(Future.sequence(promises.slice(0,3).map(_._2.future)), 3.seconds.dilated)

      val ks =
        readJournal.persistenceIds()
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.actorRef(probe.ref, 'complete))(Keep.left)
          .run()

      ars slice (3, 5) foreach { ar =>
        events foreach (ar ! _)
      }

      probe.receiveN(ars.size, 3.seconds.dilated)
        .collect { case x: String => x } should contain allOf ("1", "2", "3", "4", "5")

      ks.shutdown()

      eventually {
        mongoClient.getDatabase(embedDB).listCollectionNames()
          .into(new java.util.HashSet[String]()).asScala.filter(_.startsWith("persistenceids-")) should have size 0L
      }
  }

  it should "support the current events by id query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val mat: ActorMaterializer = ActorMaterializer()

      val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

      val promise = Promise[Unit]()
      val ar = as.actorOf(props("foo", promise, 6))

      events foreach (ar ! _)

      Await.result(promise.future, 3.seconds.dilated)

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val fut = readJournal.currentEventsByPersistenceId("foo", 0L, 2L).runFold(events.toSet) { (received, ee) =>
        val asAppend = Append(ee.event.asInstanceOf[String])
        events should contain(asAppend)
        received - asAppend
      }

      Await.result(fut, 3.seconds.dilated).map(_.s) shouldBe Set("just", "a", "test", "END")
  }

  it should "support the events by id query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val mat: ActorMaterializer = ActorMaterializer()

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val promise = Promise[Unit]()
      val ar = as.actorOf(props("foo-live", promise, 3))

      val events = ("foo" :: "bar" :: "bar2" :: Nil) map Append.apply

      val probe = TestProbe()

      val ks = readJournal
        .eventsByPersistenceId("foo-live", 2L, 3L)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.actorRef(probe.ref, 'complete))(Keep.left)
        .run()

      events foreach (ar ! _)

      probe.receiveN(2, 3.seconds.dilated)
        .collect { case msg: EventEnvelope => msg }
        .toList
        .map(_.event) should contain inOrderOnly("bar", "bar2")

      ks.shutdown()
  }

  it should "support the events by id query with multiple persistent actors" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val mat: ActorMaterializer = ActorMaterializer()

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val promise = Promise[Unit]()
      val promise2 = Promise[Unit]()
      val ar = as.actorOf(props("foo-live-2a", promise, 3))
      val ar2 = as.actorOf(props("foo-live-2b", promise2, 3))

      val events = ("foo" :: "bar" :: "bar2" :: Nil) map Append.apply
      val events2 = ("just" :: "a" :: "test" :: Nil) map Append.apply

      val probe = TestProbe()
      probe.send(ar, RUAlive)
      probe.send(ar2, RUAlive)
      probe.expectMsg(IMAlive)
      probe.expectMsg(IMAlive)

      val ks = readJournal
        .eventsByPersistenceId("foo-live-2b", 0L, Long.MaxValue)
        .viaMat(KillSwitches.single)(Keep.right)
        .take(events2.size.toLong)
        .toMat(Sink.actorRef(probe.ref, 'complete))(Keep.left)
        .run()

      events foreach (ar ! _)
      events2 foreach (ar2 ! _)

      probe.receiveN(events2.size, 3.seconds.dilated)
        .collect { case msg: EventEnvelope => msg }
        .toList
        .map(_.event) should be(events2.map(_.s))

      ks.shutdown()
  }

  it should "support read 1k events from journal" taggedAs Slow in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") {
    case (as, _) =>
      import concurrent.duration._
      implicit val system: ActorSystem = as
      implicit val mat: ActorMaterializer = ActorMaterializer()

      val readJournal =
        PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

      val nrOfActors = 10
      val nrOfEvents = 100
      val promises = (1 to nrOfActors).map(idx => Promise[Unit]() -> idx)
      val ars = promises map { case (p, idx) => system.actorOf(props(s"pid-$idx", p, nrOfEvents), s"actor-$idx") }
      val (firstHalf, secondHalf) = (1 to nrOfEvents).map(eventId => Append(s"event-$eventId")).splitAt(50)

      firstHalf foreach(ev => ars foreach (_ ! ev) )

      val probe = TestProbe()

      val sources = (1 to nrOfActors) map (nr => readJournal.eventsByPersistenceId(s"pid-$nr", 0, Long.MaxValue))

      val sink = Sink.actorRef(probe.ref, "complete")

      val ks = sources.reduce(_ merge _).viaMat(KillSwitches.single)(Keep.right).toMat(sink)(Keep.left).run()

      secondHalf foreach(ev => ars foreach (_ ! ev) )

      implicit val ec: ExecutionContextExecutor = as.dispatcher

      val done = Future.sequence(promises.map(_._1.future))
      Await.result(done, 10.seconds.dilated)

      probe.receiveN(nrOfActors * nrOfEvents, 10.seconds.dilated) should have size (nrOfActors.toLong * nrOfEvents)
      ks.shutdown()
  }
}
