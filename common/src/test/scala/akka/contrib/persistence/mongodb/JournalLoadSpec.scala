package akka.contrib.persistence.mongodb

import akka.actor._
import akka.persistence.PersistentActor
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._

abstract class JournalLoadSpec(extensionClass: Class[_]) extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  import ConfigLoanFixture._

  override def embedDB = "load-test"

  override def beforeAll() {
    doBefore()
  }

  override def afterAll() {
    doAfter()
  }

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://localhost:$embedConnectionPort/$embedDB"
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

  def actorProps(id: String): Props = Props(new CounterPersistentActor(id))

  sealed trait Command
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

  class CounterPersistentActor(id: String) extends PersistentActor with ActorLogging {

    var state = CounterState(0)

    override def receiveRecover: Receive = {
      case x:Int => (1 to x).foreach(_ => state.event(IncEvent))
    }

    override def receiveCommand: Receive = {
      case IncBatch(count) =>
        persistAll((1 to count).map(_ => 1)){ nbr =>
          state.event(IncEvent)
        }
      case Stop =>
        sender() ! state.counter
        context.stop(self)
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
      case d: FiniteDuration =>
        context.setReceiveTimeout(d)
        log.info(s"Setting receive timeout to $d")
      case x:Int =>
        accum += x
      case Terminated(ar) =>
        remaining = remaining - ar
        if (remaining.isEmpty) {
          result.success(accum)
          context.stop(self)
          log.info("Shutting down")
        }
      case ReceiveTimeout =>
        actors foreach (_ ! Stop)
        context.setReceiveTimeout(Duration.Inf)
        log.info("Sending stop to actors")
    }
  }

  val maxActors = 1000
  val batches = 10
  val commandsPerBatch = 100
  val persistenceIds = (1 to maxActors).map(x => s"$x")

  def startPersistentActors(as: ActorSystem) =
    persistenceIds.map(nm => as.actorOf(actorProps(nm),s"counter-$nm")).toSet

  "A mongo persistence driver" should "insert journal records at a rate faster than 10000/s" in withConfig(config(extensionClass), "load-test") { as =>
    val start = System.currentTimeMillis
    val actors = startPersistentActors(as)
    val thousand = 1 to commandsPerBatch
    val result = Promise[Long]()
    val accumulator = as.actorOf(Props(new Accumulator(actors, result)),"accumulator")

    (1 to batches).foreach(_ => actors foreach(ar => ar ! IncBatch(thousand.size)))  // 1MM

    accumulator ! 5.seconds

    val total = Await.result(result.future, 60.seconds)

    val time = System.currentTimeMillis - start
    (total / (time / 1000.0)) should be >= 10000.0
    println(s"$total events: $time ms ... ${total/(time / 1000.0)}")
  }

  it should "recover in less than 20 seconds" in withConfig(config(extensionClass), "load-test") { as =>
    val start = System.currentTimeMillis
    val actors = startPersistentActors(as)
    val result = Promise[Long]()
    val accumulator = as.actorOf(Props(new Accumulator(actors, result)),"accumulator")
    accumulator ! 1.nanosecond
    val total = Await.result(result.future, 60.seconds)

    val time = System.currentTimeMillis - start
    println(s"$total events: ${time/1000.0}s to recover")
    time should be <= 20000L
  }
}
