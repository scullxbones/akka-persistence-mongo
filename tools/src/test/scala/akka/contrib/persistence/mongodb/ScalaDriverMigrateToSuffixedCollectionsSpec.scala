package akka.contrib.persistence.mongodb

import akka.actor.{ActorSystem, Props}
import akka.contrib.persistence.mongodb.RxStreamsInterop._
import akka.persistence.PersistentActor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.{Await, Future, Promise}

class ScalaDriverMigrateToSuffixedCollectionsSpec extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def embedDB = s"migrate-to-suffixed-collections-test"

  override def afterAll(): Unit = cleanup()

  def config(extendedConfig: String = ""): Config = ConfigFactory.parseString(s"""
   |akka.contrib.persistence.mongodb.mongo.driver = "${classOf[ScalaDriverPersistenceExtension].getName}"
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
    $extendedConfig
   |""".stripMargin).withFallback(ConfigFactory.defaultReference())

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

  "A migration process" should "migrate journal to suffixed collections names" in {
    import concurrent.duration._

    // Populate database
    val system1: ActorSystem = ActorSystem("prepare-migration", config())
    implicit val mat1: ActorMaterializer = ActorMaterializer()(system1)
    val ec1 = system1.dispatcher

    val promises = ("foo1" :: "foo2" :: "foo3" :: "foo4" :: "foo5" :: Nil).map(id => id -> Promise[Unit]())
    val ars = promises.map { case (id, p) => system1.actorOf(props(id, p), s"migrate-persistenceId-$id") }

    val end = Append("END")
    ars foreach (_ ! end)

    val futures = promises.map { case (_, p) => p.future }
    val count = Await.result(Future.fold(futures)(0) { case (cnt, _) => cnt + 1 }(ec1), 10.seconds.dilated(system1))
    count shouldBe 5

    val underTest1 = new ScalaMongoDriver(system1, config())
    Await.result(
      Source.fromFuture(underTest1.journal)
        .flatMapConcat(_.countDocuments().asAkka)
          .runWith(Sink.head)(mat1),
      10.seconds.dilated(system1)) shouldBe 5

    Await.ready(Future(underTest1.closeConnections())(ec1), 10.seconds.dilated(system1))
    system1.terminate()
    Await.ready(system1.whenTerminated, 3.seconds)

    // perform migration
    val configExtension = SuffixCollectionNamesTest.extendedConfig
    val system2 = ActorSystem("migration", config(configExtension))
    implicit val mat2: ActorMaterializer = ActorMaterializer()(system2)
    val ec2 = system2.dispatcher

    val migrate = new ScalaDriverMigrateToSuffixedCollections(system2)
    Await.ready(migrate.migrateToSuffixCollections, 10.seconds.dilated(system2))

    system2.terminate()
    Await.ready(system2.whenTerminated, 3.seconds)

    // checking...
    val system3 = ActorSystem("check-migration", config(configExtension))
    implicit val mat3: ActorMaterializer = ActorMaterializer()(system3)
    val ec3 = system3.dispatcher

    val underTest3 = new ScalaMongoDriver(system3, config(configExtension))
    Await.result(
      Source.fromFuture(underTest3.journal)
        .flatMapConcat(_.countDocuments().asAkka)
        .runWith(Sink.head)(mat3),
      10.seconds.dilated(system3)) shouldBe 0

    Await.result(
      underTest3.db.listCollectionNames().asAkka
        .runWith(Sink.seq)(mat3),
      10.seconds.dilated(system3)) should contain allOf ("akka_persistence_journal_foo1-test",
                                                        "akka_persistence_journal_foo2-test",
                                                        "akka_persistence_journal_foo3-test",
                                                        "akka_persistence_journal_foo4-test",
                                                        "akka_persistence_journal_foo5-test")

    import akka.contrib.persistence.mongodb.JournallingFieldNames._

    (1 to 5) foreach { id =>
      Await.result(
        Source.fromFuture(underTest3.getJournal(s"foo$id"))
          .flatMapConcat(_.countDocuments(org.mongodb.scala.model.Filters.equal(PROCESSOR_ID, s"foo$id")).asAkka)
          .runWith(Sink.head)(mat3),
        10.seconds.dilated(system3)) shouldBe 1
      (1 to 5) filterNot (_ == id) foreach { otherId =>
        Await.result(
          Source.fromFuture(underTest3.getJournal(s"foo$otherId"))
            .flatMapConcat(_.countDocuments(org.mongodb.scala.model.Filters.equal(PROCESSOR_ID, s"foo$id")).asAkka)
            .runWith(Sink.head)(mat3),
          10.seconds.dilated(system3)) shouldBe 0
      }
    }

    Await.ready(Future(underTest3.closeConnections())(ec3), 10.seconds.dilated(system3))
    system3.terminate()
    Await.ready(system3.whenTerminated, 3.seconds)
    ()

  }
}
