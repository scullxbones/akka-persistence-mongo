package akka.contrib.persistence.mongodb

import akka.actor.{PoisonPill, Props}
import akka.persistence.PersistentActor
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.concurrent.{Future, Await, Promise}

abstract class ReadJournalSpec[A <: MongoPersistenceExtension](extensionClass: Class[A]) extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll with Eventually {

  import ConfigLoanFixture._

  override def embedDB = "read-journal-test"

  override def beforeAll(): Unit = {
    doBefore()
  }

  override def afterAll(): Unit = {
    doAfter()
  }

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.journal-read-fill-limit = 10
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://localhost:$embedConnectionPort/$embedDB"
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
    |""".stripMargin).withFallback(ConfigFactory.defaultReference())

  def props(id: String, promise: Promise[Unit]) = Props(new Persistent(id, promise))

  case class Append(s: String)

  class Persistent(val persistenceId: String, completed: Promise[Unit]) extends PersistentActor {
    var events = Vector.empty[String]

    override def receiveRecover: Receive = {
      case s: String => events = events :+ s
    }

    override def receiveCommand: Receive = {
      case Append(s) => persist(s){str =>
        events = events :+ str
        if (str == "END") {
          completed.success(())
          self ! PoisonPill
        }
      }
    }
  }


  "A read journal" should "support the journal dump query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_) =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()

    val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

    val promise = Promise[Unit]()
    val ar = as.actorOf(props("foo",promise))

    events foreach (ar ! _)

    Await.result(promise.future, 10.seconds)

    val readJournal =
      PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

    val fut = readJournal.allEvents().runFold(events.toSet){ (received, ee) =>
      println(s"ee = $ee")
      val asAppend = Append(ee.event.asInstanceOf[String])
      events should contain (asAppend)
      received - asAppend
    }

    Await.result(fut,10.seconds) shouldBe empty
  }

  it should "support the all persistence ids query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_)  =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()

    val promises = ("1" :: "2" :: "3" :: "4" :: "5" :: Nil).map(id => id -> Promise[Unit]())
    val ars = promises.map{ case (id,p) => as.actorOf(props(id,p)) }

    val end = Append("END")
    ars foreach (_ ! end)

    implicit val ec = as.dispatcher
    val futures = promises.map{case(_,p)=>p.future}
    Await.result(Future.sequence(futures), 10.seconds)

    val readJournal =
      PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

    val fut = readJournal.currentPersistenceIds().runFold(Seq.empty[String])(_ :+ _)

    Await.result(fut,10.seconds) should contain allOf("1","2","3","4","5")
  }

  it should "support the events by id query" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_) =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()

    val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

    val promise = Promise[Unit]()
    val ar = as.actorOf(props("foo",promise))

    events foreach (ar ! _)

    Await.result(promise.future, 10.seconds)

    val readJournal =
      PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

    val fut = readJournal.currentEventsByPersistenceId("foo",0L,2L).runFold(events.toSet){(received, ee) =>
      println(s"Received so far $received, envelope = $ee")
      val asAppend = Append(ee.event.asInstanceOf[String])
      events should contain (asAppend)
      received - asAppend
    }

    Await.result(fut,10.seconds).map(_.s) shouldBe Set("just","a","test","END")
  }
}
