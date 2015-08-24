package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner
import scala.concurrent._
import duration._
import ExecutionContext.Implicits.global

import ConfigLoanFixture._

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceDriverShutdownSpec extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  override def beforeAll() = doBefore()
  override def afterAll() = doAfter()

  val shutdownConfig = ConfigFactory.parseString(
    s"""
        |akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://localhost:$embedConnectionPort/"
        | db = "shutdown-spec"
        |}
      """.stripMargin)

  class MockRxMongoPersistenceDriver(actorSystem:ActorSystem) extends RxMongoDriver(actorSystem) {
    def showCollections = db.collectionNames
  }


  "An rxmongo driver" should "close the mongodb connection pool on actor system shutdown" in withConfig(shutdownConfig,"shutdown-config") { actorSystem =>
    val underTest = new MockRxMongoPersistenceDriver(actorSystem)
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated, 10.seconds)
    intercept[IllegalStateException] {
      Await.result(underTest.showCollections,3.seconds).size
    }
    ()
  }


  it should "reconnect if a new driver is created" in withConfig(shutdownConfig,"shutdown-config") { actorSystem =>
    val underTest = new MockRxMongoPersistenceDriver(actorSystem)
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated, 10.seconds)

    val test2 = ActorSystem("test2",shutdownConfig)
    try {
      val underTest2 = new MockRxMongoPersistenceDriver(test2)
      Await.result(underTest2.showCollections, 3.seconds).size should be(0)
    } finally {
      test2.terminate()
      ()
    }
    ()
  }
}

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceDriverAuthSpec extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  override def beforeAll() = doBefore()
  override def afterAll() = doAfter()

  override def embedDB = "admin"
  override def auth = new AuthenticatingCommandLinePostProcessor()

  val authMode = if(envMongoVersion.contains("3.0")) "?authMode=scram-sha1" else ""

  val authConfig = ConfigFactory.parseString(
    s"""
        |akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://admin:password@localhost:$embedConnectionPort/admin$authMode"
        |}
      """.stripMargin)

  "A secured mongodb instance" should "be connectable via user and pass" in withConfig(authConfig,"authentication-config") { actorSystem =>
    val underTest = new RxMongoDriver(actorSystem)
    val collections = Await.result(underTest.db.collectionNames,3.seconds)
    collections should contain ("system.users")
    ()
  }
}