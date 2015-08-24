package akka.contrib.persistence.mongodb

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.persistence.PersistentActor
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import scala.util.Try

abstract class JournalLoadSpec(extensionClass: Class[_]) extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  import ConfigLoanFixture._

  override def embedDB = "load-test"

  override def beforeAll(): Unit = {
    doBefore()
  }

  override def afterAll(): Unit = {
    doAfter()
  }

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://localhost:$embedConnectionPort/$embedDB"
    |akka.contrib.persistence.mongodb.mongo.breaker.timeout.call = 0s
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
    |""".stripMargin)

  def actorProps(id: String, eventCount: Int, atMost: FiniteDuration = 60.seconds): Props =
    Props(new CounterPersistentActor(id, eventCount, atMost))

  sealed trait Command
  case class SetTarget(ar: ActorRef) extends Command
  case class IncBatch(n: Int) extends Command
  case object Stop extends Command

  sealed trait Event
  case object IncEvent extends Event

  case class CounterState(init: Int = 0) {
    var counter = init

    def event(ev: Event): CounterState = ev match {
      case IncEvent =>
        counter += 1
        this
    }
  }

  class CounterPersistentActor(id: String, eventCount: Int, atMost: FiniteDuration) extends PersistentActor with ActorLogging {

    var state = CounterState(0)
    var inflight = new AtomicInteger(eventCount)
    var accumulator: Option[ActorRef] = None

    override def receiveRecover: Receive = {
      case x:Int => (1 to x).foreach(_ => state.event(IncEvent))
    }

    private def eventHandler(int: Int): Unit = {
      state.event(IncEvent)
      checkShutdown()
    }

    private def checkShutdown(): Unit = if (inflight.decrementAndGet() < 1) {
      accumulator.foreach(_ ! state.counter)
      context.stop(self)
    }

    override def receiveCommand: Receive = {
      case SetTarget(ar) =>
        accumulator = Option(ar)
        context.setReceiveTimeout(atMost)
      case IncBatch(count) =>
        persistAll((1 to count).map(_ => 1))(eventHandler)
      case ReceiveTimeout =>
        if (recoveryFinished) {
          inflight.set(0)
          checkShutdown()
        }
    }

    override def persistenceId: String = s"counter-$id"
  }

  class Accumulator(actors: Set[ActorRef], result: Promise[Long]) extends Actor with ActorLogging {
    var accum = 0L
    var remaining = actors

    override def preStart() = {
      actors foreach context.watch
      log.info(s"Watching ${actors.size} actors")
    }

    def receive = {
      case x:Int =>
        accum += x
      case Terminated(ar) =>
        remaining = remaining - ar
        if (remaining.isEmpty) {
          result.success(accum)
          context.stop(self)
          log.info("Shutting down")
        }
    }
  }

  val maxActors = 100
  val batches = 10
  val commandsPerBatch = 10
  val persistenceIds = (1 to maxActors).map(x => s"$x")

  def startPersistentActors(as: ActorSystem, eventsPer: Int, maxDuration: FiniteDuration) =
    persistenceIds.map(nm => as.actorOf(actorProps(nm, eventsPer, maxDuration),s"counter-$nm")).toSet

  "A mongo persistence driver" should "insert journal records at a rate faster than 10000/s" in withConfig(config(extensionClass), "load-test") { as =>
    val thousand = 1 to commandsPerBatch
    val actors = startPersistentActors(as, commandsPerBatch * batches, 60.seconds)
    val result = Promise[Long]()
    val accumulator = as.actorOf(Props(new Accumulator(actors, result)),"accumulator")
    actors.foreach(_ ! SetTarget(accumulator))

    val start = System.currentTimeMillis
    (1 to batches).foreach(_ => actors foreach(ar => ar ! IncBatch(commandsPerBatch)))

    val total = Try(Await.result(result.future, 60.seconds))

    val time = System.currentTimeMillis - start
    // (total / (time / 1000.0)) should be >= 10000.0
    println(s"$total events: $time ms ... ${total.map(_/(time / 1000.0)).map(_.toString).getOrElse("N/A")}")
  }

  it should "recover in less than 20 seconds" in withConfig(config(extensionClass), "load-test") { as =>
    val start = System.currentTimeMillis
    val actors = startPersistentActors(as, 0, 100.milliseconds)
    val result = Promise[Long]()
    val accumulator = as.actorOf(Props(new Accumulator(actors, result)),"accumulator")
    actors.foreach(_ ! SetTarget(accumulator))

    val total = Try(Await.result(result.future, 60.seconds))

    val time = System.currentTimeMillis - start
    println(s"$total events: ${time/1000.0}s to recover")
    // time should be <= 20000L
  }
}
