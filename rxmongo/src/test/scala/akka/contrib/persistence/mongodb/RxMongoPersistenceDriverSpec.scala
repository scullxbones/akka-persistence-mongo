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
class RxMongoPersistenceDriverShutdownSpec extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll {

  override def afterAll() = cleanup()

  override def embedDB = "rxmongo-shutdown"

  val shutdownConfig = ConfigFactory.parseString(
    s"""
        |akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://$host:$noAuthPort/$embedDB"
        | db = "shutdown-spec"
        |}
      """.stripMargin)

  class MockRxMongoPersistenceDriver(actorSystem:ActorSystem) extends RxMongoDriver(actorSystem, ConfigFactory.empty()) {
    def showCollections = db.collectionNames
  }


  "An rxmongo driver" should "close the mongodb connection pool on actor system shutdown" in withConfig(shutdownConfig,"akka-contrib-mongodb-persistence-journal","shutdown-config") { case (actorSystem,_) =>
    val underTest = new MockRxMongoPersistenceDriver(actorSystem)
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated, 10.seconds)
    intercept[IllegalStateException] {
      Await.result(underTest.showCollections,3.seconds).size
    }
    ()
  }


  it should "reconnect if a new driver is created" in withConfig(shutdownConfig,"akka-contrib-mongodb-persistence-journal","shutdown-config") { case (actorSystem,_) =>
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
class RxMongoPersistenceDriverAuthSpec extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll {

  val authMode = if( "3.0" :: "3.2" :: Nil exists envMongoVersion.contains) "?authMode=scram-sha1" else ""

  val authConfig = ConfigFactory.parseString(
    s"""
        |akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://admin:password@$host:$authPort/admin$authMode"
        |}
      """.stripMargin)

  "A secured mongodb instance" should "be connectable via user and pass" in withConfig(authConfig,"akka-contrib-mongodb-persistence-journal","authentication-config") { case (actorSystem, config) =>
    val underTest = new RxMongoDriver(actorSystem, config)
    val collections = Await.result(underTest.db.collectionNames,3.seconds)
    collections should contain ("system.users")
    ()
  }
}