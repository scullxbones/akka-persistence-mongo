package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.ConfigLoanFixture._
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceDriverShutdownSpec extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll {

  override def afterAll(): Unit = cleanup()

  override def embedDB = "rxmongo-shutdown"

  val shutdownConfig: Config = ConfigFactory.parseString(
    s"""
        |akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://$host:$noAuthPort/$embedDB"
        | db = "shutdown-spec"
        |}
      """.stripMargin)

  class MockRxMongoPersistenceDriver(actorSystem:ActorSystem) extends RxMongoDriver(actorSystem, ConfigFactory.empty(), new RxMongoDriverProvider(actorSystem)) {
    def showCollections: Future[List[String]] = db.flatMap(_.collectionNames)
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

  import ExecutionContext.Implicits.global

  val authMode: String = {
    if (envMongoVersion contains "2.6") "?authenticationMechanism=mongocr"
    else "?authenticationMechanism=scram-sha1"
  }

  val authConfig: Config = ConfigFactory.parseString(
    s"""
        |akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://admin:password@$host:$authPort/admin$authMode"
        |}
      """.stripMargin)

  "A secured mongodb instance" should "be connectable via user and pass" in withConfig(authConfig,"akka-contrib-mongodb-persistence-journal","authentication-config") { case (actorSystem, config) =>
    val underTest = new RxMongoDriver(actorSystem, config, new RxMongoDriverProvider(actorSystem))
    val collections = Await.result(underTest.db.flatMap(_.collectionNames),3.seconds)
    collections should contain ("system.users")
    ()
  }
}
