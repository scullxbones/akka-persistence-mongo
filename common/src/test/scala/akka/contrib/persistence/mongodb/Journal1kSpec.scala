package akka.contrib.persistence.mongodb

import akka.actor.{ExtendedActorSystem, Props}
import akka.persistence.PersistentActor
import akka.serialization.{SerializerWithStringManifest, Serializer}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures

abstract class Journal1kSpec(extensionClass: Class[_]) extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll with ScalaFutures {

  import ConfigLoanFixture._

  override def embedDB = "1k-test"

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

  object Counter {
    def props = Props(new Counter)
    case object Inc
    case object GetCounter
    case object Shutdown
  }

  class Counter extends PersistentActor{
    import Counter._
    var counter = 0

    override def receiveRecover: Receive = {
      case Inc => counter += 1
      case x:Int => counter += x
    }

    override def receiveCommand: Receive = {
      case Inc =>
        persist(1) { _ =>
          counter += 1
        }
      case GetCounter => sender() ! counter
      case Shutdown => context stop self
    }

    override def persistenceId: String = self.path.name
  }

  val id = "123"
  import Counter._
  import akka.pattern._

  import concurrent.duration._

  "A counter" should "persist the counter to 1000" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "1k-test") { case (as,config) =>
    implicit val askTimeout = Timeout(2.minutes)
    val counter = as.actorOf(Counter.props, id)
    (1 to 1000).foreach(_ => counter ! Inc)
    val result = (counter ? GetCounter).mapTo[Int]
    whenReady(result, timeout(askTimeout.duration + 10.seconds)) { onek =>
      onek shouldBe 1000
    }
  }

  it should "restore the counter back to 1000" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "1k-test") { case (as, config) =>
    implicit val askTimeout = Timeout(2.minutes)
    val counter = as.actorOf(Counter.props, id)
    val result = (counter ? GetCounter).mapTo[Int]
    whenReady(result, timeout(askTimeout.duration + 10.seconds)) { onek =>
      onek shouldBe 1000
    }
  }
}
