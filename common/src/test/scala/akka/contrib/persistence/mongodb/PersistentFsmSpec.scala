package akka.contrib.persistence.mongodb

import akka.actor.{ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import akka.persistence.fsm.PersistentFSM
import akka.persistence.fsm.PersistentFSM.FSMState
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
  * From [docs](http://doc.akka.io/docs/akka/2.4/scala/persistence.html#Persistent_FSM)
  */
object PersistentFsmSpec {
  sealed trait Command
  case class AddItem(item: Item) extends Command
  case object Buy extends Command
  case object Leave extends Command
  case object GetCurrentCart extends Command

  sealed trait UserState extends FSMState
  case object LookingAround extends UserState {
    override def identifier: String = "Looking Around"
  }
  case object Shopping extends UserState {
    override def identifier: String = "Shopping"
  }
  case object Inactive extends UserState {
    override def identifier: String = "Inactive"
  }
  case object Paid extends UserState {
    override def identifier: String = "Paid"
  }

  sealed trait DomainEvent extends Serializable
  case class ItemAdded(item: Item) extends DomainEvent
  case object OrderExecuted extends DomainEvent
  case object OrderDiscarded extends DomainEvent

  case class Item(id: String, name: String, price: Float) extends Serializable

  sealed trait ShoppingCart {
    def addItem(item: Item): ShoppingCart
    def empty(): ShoppingCart
  }
  case object EmptyShoppingCart extends ShoppingCart {
    def addItem(item: Item) = NonEmptyShoppingCart(item :: Nil)
    def empty():ShoppingCart = this
  }
  case class NonEmptyShoppingCart(items: Seq[Item]) extends ShoppingCart {
    def addItem(item: Item) = NonEmptyShoppingCart(items :+ item)
    def empty():ShoppingCart = EmptyShoppingCart
  }

  case class PurchaseWasMade(items:Seq[Item])
  case object ShoppingCardDiscarded

  def props(reportActor: ActorRef, id: String) = Props(new TestActor(reportActor, id))

  class TestActor(reportActor: ActorRef, val persistenceId: String) extends PersistentFSM[UserState, ShoppingCart, DomainEvent] with ActorLogging {

    override implicit def domainEventClassTag: ClassTag[DomainEvent] = ClassTag(classOf[DomainEvent])

    override def applyEvent(event: DomainEvent, cartBeforeEvent: ShoppingCart): ShoppingCart = {
      event match {
        case ItemAdded(item) ⇒ cartBeforeEvent.addItem(item)
        case OrderExecuted   ⇒ cartBeforeEvent
        case OrderDiscarded  ⇒ cartBeforeEvent.empty()
      }
    }

    override def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      log.error(cause,s"Recovery failed at event $event")
    }

    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(cause,s"Persist of event $event failed at seq=$seqNr")
    }

    override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
      log.error(reason,s"Restarting due to message $message")
    }

    startWith(LookingAround, EmptyShoppingCart)

    when(LookingAround) {
      case Event(AddItem(item), _) ⇒
        goto(Shopping) applying ItemAdded(item) forMax 1.second
      case Event(GetCurrentCart, data) ⇒
        stay replying data
    }

    when(Shopping) {
      case Event(AddItem(item), _) ⇒
        stay applying ItemAdded(item) forMax 1.second
      case Event(Buy, _) ⇒
        goto(Paid) applying OrderExecuted andThen {
          case NonEmptyShoppingCart(items) ⇒
            reportActor ! PurchaseWasMade(items)
            saveStateSnapshot()
          case EmptyShoppingCart ⇒ saveStateSnapshot()
        }
      case Event(Leave, _) ⇒
        stop applying OrderDiscarded andThen {
          _ ⇒
            reportActor ! ShoppingCardDiscarded
            saveStateSnapshot()
        }
      case Event(GetCurrentCart, data) ⇒
        stay replying data
      case Event(StateTimeout, _) ⇒
        goto(Inactive) forMax 2.seconds
    }

    when(Inactive) {
      case Event(AddItem(item), _) ⇒
        goto(Shopping) applying ItemAdded(item) forMax 1.second
      case Event(StateTimeout, _) ⇒
        stop applying OrderDiscarded andThen (_ ⇒ reportActor ! ShoppingCardDiscarded)
    }

    when(Paid) {
      case Event(Leave, _) ⇒ stop()
      case Event(GetCurrentCart, data) ⇒
        stay replying data
    }

    onTransition {
      case a -> b =>
        log.info(s"From ${a.identifier} to ${b.identifier}")
    }
  }
}

abstract class PersistentFsmSpec(extensionClass: Class[_], database: String, extendedConfig: String = "|")
  extends TestKit(ActorSystem("unit-test"))
    with BaseUnitTest with ContainerMongo with BeforeAndAfterAll with ImplicitSender {

  import ConfigLoanFixture._
  import PersistentFsmSpec._

  override def embedDB = s"persistent-fsm-$database"

  override def beforeAll(): Unit = cleanup()

  def config(extensionClass: Class[_]): Config =
    ConfigFactory.parseString(s"""
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

  "A mongo persistence driver" should
    "support persistent FSM purchase sequence" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "persistent-fsm") { case (as, config) =>
      implicit val system = as

      val probe = TestProbe()
      val actor = as.actorOf(props(probe.ref, "persistent-fsm-1"),"1")
      probe.watch(actor)

      val items = List(1,2,3)
        .map(i => Item(s"id$i",s"name$i",i.toFloat))

      items.foreach(actor ! AddItem(_))
      actor ! Buy

      probe.expectMsg(PurchaseWasMade(items))
      actor ! Leave
      probe.expectTerminated(actor)
  }

  it should
    "support persistent FSM shop and discard" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "persistent-fsm") { case (as, config) =>
      implicit val system = as

      val probe = TestProbe()
      val actor = as.actorOf(props(probe.ref, "persistent-fsm-2"),"2")


      val items = List(1,2,3)
        .map(i => Item(s"id$i",s"name$i",i.toFloat))

      items.foreach(actor ! AddItem(_))
      actor tell(GetCurrentCart, probe.ref)

      probe.expectMsg(NonEmptyShoppingCart(items))
      actor tell(Leave, probe.ref)

      probe.expectMsg(ShoppingCardDiscarded)
  }

  it should
    "support persist and recovery of FSM cart" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "persistent-fsm") { case (as, config) =>
      implicit val system = as

      val probe = TestProbe()
      val actor = as.actorOf(props(probe.ref, "persistent-fsm-3"),"3a")
      probe.watch(actor)

      val items = List(1,2,3)
        .map(i => Item(s"id$i",s"name$i",i.toFloat))

      items.foreach(actor ! AddItem(_))
      actor tell(GetCurrentCart, probe.ref)

      probe.expectMsg(NonEmptyShoppingCart(items))
      actor ! PoisonPill

      probe.expectTerminated(actor)

      val recovery = as.actorOf(props(probe.ref, "persistent-fsm-3"),"3b")
      recovery tell(GetCurrentCart, probe.ref)

      probe.expectMsg(NonEmptyShoppingCart(items))
  }
}
