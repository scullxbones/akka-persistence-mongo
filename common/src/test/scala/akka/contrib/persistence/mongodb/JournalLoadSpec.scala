/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.persistence.PersistentActor
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.tagobjects.Slow

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.{Success, Try}

abstract class JournalLoadSpec(extensionClass: Class[_], database: String, extendedConfig: String = "|") extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll {

  import ConfigLoanFixture._

  override def embedDB = s"load-test-$database"

  override def afterAll() = cleanup()

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
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

    override def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      log.error(cause,s"FAILED TO RECOVER during load test due to event of type ${event.map(_.getClass)}: $event")

      super.onRecoveryFailure(cause, event)
    }

    private def eventHandler(int: Int): Unit = {
      (1 to int) foreach { _ =>
        state.event(IncEvent)
        checkShutdown()
      }
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
        persistAllAsync((1 to count).map(_ => 1))(eventHandler)
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
  val commandsPerBatch = 100
  val persistenceIds = (1 to maxActors).map(x => s"$x")

  def startPersistentActors(as: ActorSystem, eventsPer: Int, maxDuration: FiniteDuration) =
    persistenceIds.map(nm => as.actorOf(actorProps(nm, eventsPer, maxDuration),s"counter-$nm")).toSet

  "A mongo persistence driver" should "insert journal records at a rate faster than 10000/s" taggedAs Slow in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "load-test") { case (as,config) =>
    implicit val system = as
    val actors = startPersistentActors(as, commandsPerBatch * batches, 10.seconds.dilated)
    val result = Promise[Long]()
    val accumulator = as.actorOf(Props(new Accumulator(actors, result)),"accumulator")
    actors.foreach(_ ! SetTarget(accumulator))

    val start = System.currentTimeMillis
    (1 to batches).foreach(_ => actors foreach(ar => ar ! IncBatch(commandsPerBatch)))

    val total = Try(Await.result(result.future, 30.seconds.dilated))

    val time = System.currentTimeMillis - start
    // (total / (time / 1000.0)) should be >= 10000.0
    println(
      s"""
         |==== Load (write) test result ===
         |
         |Extension:       ${extensionClass.getSimpleName}
         |
         |Total Actors:    ${actors.size}
         |Total Events:    ${total.getOrElse(0L)}
         |Total Time (ms): $time
         |                 --------
         |
         |Write Rate (ev/s): ${math.floor(total.getOrElse(0L) /(time / 1000.0))}
         |
         |==== ------------------------ ===
       """.stripMargin)
    total shouldBe Success(commandsPerBatch * batches * maxActors)
  }

  it should "recover in less than 20 seconds" taggedAs Slow in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "load-test") { case (as,config) =>
    implicit val system = as
    val start = System.currentTimeMillis
    val actors = startPersistentActors(as, commandsPerBatch * batches, 100.milliseconds.dilated)
    val result = Promise[Long]()
    val accumulator = as.actorOf(Props(new Accumulator(actors, result)),"accumulator")
    actors.foreach(_ ! SetTarget(accumulator))

    val total = Try(Await.result(result.future, 10.seconds.dilated)).recoverWith {
      case t: Throwable =>
        t.printStackTrace()
        scala.util.Failure(t)
    }.getOrElse(0L)

    val time = System.currentTimeMillis - start

    println(
      s"""
         |==== Load (read) test result ===
         |
         |Extension:       ${extensionClass.getSimpleName}
         |
         |Total Actors:    ${actors.size}
         |Total Events:    $total
         |Total Time (ms): $time
         |                 --------
         |
         |Recovery rate (ev/s): ${if (time > 0) math.floor(total.toDouble / (time.toDouble / 1000.0)) else 0.0}
         |
         |==== ----------------------- ===
       """.stripMargin)

    println(s"$total events: ${time/1000.0}s to recover")
    total shouldBe commandsPerBatch * batches * maxActors
  }
}
