package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.ServerAddress
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverSpec extends BaseUnitTest{

  "A collection of mongo urls" should "be transformed to ServerAddresses" in {
    val mongoUrls = List("localhost:123","localhost:345","localhost:27017")
    val converted = CasbahPersistenceDriver.convertListOfStringsToListOfServerAddresses(mongoUrls)
    converted should have size 3
    converted should equal (List(new ServerAddress("localhost",123), new ServerAddress("localhost",345), new ServerAddress("localhost",27017)))
  }

}

import ConfigLoanFixture._

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverShutdownSpec extends BaseUnitTest with EmbeddedMongo {

  val shutdownConfig = ConfigFactory.parseString(
    s"""|mongo {
        | urls = [ "localhost:$embedConnectionPort" ]
        | db = "shutdown-spec"
        |}
      """.stripMargin)

  "A casbah driver" should "close the mongodb connection pool on actor system shutdown" in withConfig(shutdownConfig) { actorSystem =>
    val underTest = new CasbahMongoDriver(actorSystem)
    underTest.actorSystem.shutdown()
    underTest.actorSystem.awaitTermination(10.seconds)
    intercept[IllegalStateException] {
      underTest.db.stats()
    }
  }


  it should "reconnect if a new driver is created" in withConfig(shutdownConfig)  { actorSystem =>
    val underTest = new CasbahMongoDriver(actorSystem)
    underTest.actorSystem.shutdown()
    underTest.actorSystem.awaitTermination(10.seconds)

    val underTest2 = new CasbahMongoDriver(ActorSystem("test2",shutdownConfig))
    underTest2.db.stats() // Should not throw exception
  }
}

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverAuthSpec extends BaseUnitTest with EmbeddedMongo {

  override def embedDB = "admin"
  override def auth = new AuthenticatingCommandLinePostProcessor()

  val authConfig = ConfigFactory.parseString(
    s"""
        |        mongo {
        |          urls = [ "localhost:$embedConnectionPort" ]
        |          db = "admin"
        |          username = "admin"
        |          password = "password"
        |        }
      """.stripMargin)

  "A secured mongodb instance" should "be connectable via user and pass" in withConfig(authConfig) { actorSystem =>
    val underTest = new CasbahMongoDriver(actorSystem)
    val collections = underTest.db.collectionNames()
    collections.size should be (3)
    collections should contain ("system.indexes")
  }
}
