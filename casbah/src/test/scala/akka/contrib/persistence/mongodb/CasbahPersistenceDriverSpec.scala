package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner
import scala.concurrent.Await
import scala.concurrent.duration._

import ConfigLoanFixture._

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverShutdownSpec extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  override def beforeAll() {
    doBefore()
  }

  override def afterAll() {
    doAfter()
  }

  val shutdownConfig = ConfigFactory.parseString(
    s"""|akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://localhost:$embedConnectionPort/shutdown-spec"
        | db = "shutdown-spec"
        |}
      """.stripMargin)

  "A casbah driver" should "close the mongodb connection pool on actor system shutdown" in withConfig(shutdownConfig) { actorSystem =>
    val underTest = new CasbahMongoDriver(actorSystem)
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated,10.seconds)
    intercept[IllegalStateException] {
      underTest.db.stats()
    }
  }


  it should "reconnect if a new driver is created" in withConfig(shutdownConfig)  { actorSystem =>
    val underTest = new CasbahMongoDriver(actorSystem)
    underTest.db.collectionNames()
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated,10.seconds)

    val newAs:ActorSystem = ActorSystem("test2",shutdownConfig)
    val underTest2 = new CasbahMongoDriver(newAs)
    underTest2.db.collectionNames()
    newAs.terminate()
  }
}

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverAuthSpec extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  override def beforeAll() {
    doBefore()
  }

  override def afterAll() {
    doAfter()
  }

  override def embedDB = "admin"
  override def auth = new AuthenticatingCommandLinePostProcessor()

  val authConfig = ConfigFactory.parseString(
    s"""
        |akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://admin:password@localhost:$embedConnectionPort/admin"
        |}
      """.stripMargin)

  "A secured mongodb instance" should "be connectable via user and pass" in withConfig(authConfig) { actorSystem =>
    val underTest = new CasbahMongoDriver(actorSystem)
    val collections = underTest.db.collectionNames()
    collections.size should be (3)
    collections should contain ("system.users")
  }
}
