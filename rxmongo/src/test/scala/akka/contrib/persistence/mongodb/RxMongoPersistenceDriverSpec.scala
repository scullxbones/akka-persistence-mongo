package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import reactivemongo.bson.BSONDocument
import reactivemongo.core.commands.Count
import scala.concurrent._
import duration._
import ExecutionContext.Implicits.global

class RxMongoPersistenceDriverSpec {

}

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceDriverShutdownSpec extends BaseUnitTest with EmbeddedMongo {

  class MockRxMongoPersistenceDriver extends RxMongoDriver(ActorSystem("shutdown-spec")) {

    override val userOverrides = ConfigFactory.parseString(
      s"""
        |mongo {
        | urls = [ "localhost:$embedConnectionPort" ]
        | db = "shutdown-spec"
        |}
      """.stripMargin).resolve()

    def showCollections = db.collectionNames
  }


  "An rxmongo driver" should "close the mongodb connection pool on actor system shutdown" in {
    val underTest = new MockRxMongoPersistenceDriver
    underTest.actorSystem.shutdown()
    underTest.actorSystem.awaitTermination(10.seconds)
    intercept[IllegalStateException] {
      Await.result(underTest.showCollections,3.seconds).size
    }
  }


  it should "reconnect if a new driver is created" in {
    val underTest = new MockRxMongoPersistenceDriver
    underTest.actorSystem.shutdown()
    underTest.actorSystem.awaitTermination(10.seconds)

    val underTest2 = new MockRxMongoPersistenceDriver
    Await.result(underTest2.showCollections,3.seconds).size should be (0)
  }
}

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceDriverAuthSpec extends BaseUnitTest with EmbeddedMongo {

  override def embedDB = "admin"
  override def auth = new AuthenticatingCommandLinePostProcessor()

  class MockRxMongoPersistenceDriver extends RxMongoDriver(ActorSystem("authentication-spec")) {

    override val userOverrides = ConfigFactory.parseString(
      s"""
        |        mongo {
        |          urls = [ "localhost:$embedConnectionPort" ]
        |          db = "admin"
        |          username = "admin"
        |          password = "password"
        |        }
      """.stripMargin).resolve()
  }

  "A secured mongodb instance" should "be connectable via user and pass" in {
    val underTest = new MockRxMongoPersistenceDriver
    val collections = Await.result(underTest.db.collectionNames,3.seconds)
    collections.size should be (3)
    collections should contain ("system.users")
  }
}