package akka.contrib.persistence.mongodb

import akka.actor.Props
import akka.contrib.persistence.mongodb.OrderIdActor.{Get, Increment}
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._


object OrderIdActor {
  sealed trait Command
  case object Increment extends Command
  case object Get extends Command

  sealed trait MyEvent
  case class Incremented(value: Int = 1) extends MyEvent

  def props = Props(new OrderIdActor)
  def name = "order-id"
}

class OrderIdActor extends PersistentActor {
  import OrderIdActor._

  private var state = 0

  private def updateState(ev: MyEvent) = ev match {
    case Incremented(value) => state += value
  }

  override def receiveRecover: Receive = {
    case e:MyEvent => updateState(e)
  }

  override def receiveCommand: Receive = {
    case Increment => persist(Incremented(1)){
      e =>
        updateState(e)
        sender() ! state
    }
    case Get => sender() ! state
  }
  override def persistenceId: String = "order-id"
}

abstract class JournalSerializableSpec(extensionClass: Class[_], database: String) extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll with ScalaFutures {
  import ConfigLoanFixture._

  override def embedDB = s"serializable-spec-$database"

  override def afterAll() = cleanup()

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://$host:$noAuthPort/$embedDB"
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
    }""".stripMargin)

  "A journal" should "support writing serializable events" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal") { case (as,_) =>
    implicit val system = as
    implicit val defaultPatience =
      PatienceConfig(timeout = 5.seconds.dilated, interval = 500.millis.dilated)

    val pa = as.actorOf(OrderIdActor.props)
    pa ! Increment
    pa ! Increment
    pa ! Increment
    pa ! Increment
    whenReady((pa ? Increment)(5.second.dilated)) {
      _ shouldBe 5
    }
  }

  it should "support restoring serializable events" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal") { case (as,_) =>
    implicit val system = as
    implicit val defaultPatience =
      PatienceConfig(timeout = 5.seconds.dilated, interval = 500.millis.dilated)

    val pa = as.actorOf(OrderIdActor.props)
    whenReady((pa ? Get)(5.second.dilated)) {
      _ shouldBe 5
    }
  }
}
