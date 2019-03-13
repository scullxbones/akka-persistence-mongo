package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.ConfigLoanFixture.withConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner

import scala.concurrent.{Await, Future, duration}
import duration._

@RunWith(classOf[JUnitRunner])
class ScalaDriverPersistenceShutdownSpec extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll {

  override def beforeAll(): Unit = cleanup()

  override def embedDB = "official-scala-shutdown"

  val shutdownConfig: Config = ConfigFactory.parseString(
    s"""
       |akka.contrib.persistence.mongodb.mongo {
       | mongouri = "mongodb://$host:$noAuthPort/$embedDB"
       | db = "$embedDB"
       |}
      """.stripMargin)

  class MockScalaPersistenceDriver(actorSystem:ActorSystem) extends ScalaMongoDriver(actorSystem, ConfigFactory.empty()) {
    def showCollections: Future[List[String]] =
      db.listCollectionNames()
        .toFuture()
        .map(_.toList)
  }


  "An official scala driver" should "close the mongodb connection pool on actor system shutdown" in withConfig(shutdownConfig,"akka-contrib-mongodb-persistence-journal","shutdown-config") { case (actorSystem,_) =>
    val underTest = new MockScalaPersistenceDriver(actorSystem)
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated, 10.seconds)
    intercept[IllegalStateException] {
      Await.result(underTest.showCollections,3.seconds).size
    }
    ()
  }


  it should "reconnect if a new driver is created" in withConfig(shutdownConfig,"akka-contrib-mongodb-persistence-journal","shutdown-config") { case (actorSystem,_) =>
    val underTest = new MockScalaPersistenceDriver(actorSystem)
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated, 10.seconds)

    val test2 = ActorSystem("test2",shutdownConfig)
    try {
      val underTest2 = new MockScalaPersistenceDriver(test2)
      Await.result(underTest2.showCollections, 3.seconds).size should be(0)
    } finally {
      test2.terminate()
      ()
    }
    ()
  }
}

@RunWith(classOf[JUnitRunner])
class ScalaDriverPersistenceAuthSpec extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll {

  val authMode: String = if( envMongoVersion.contains("2.6") ) "?authMechanism=MONGODB-CR" else "?authMechanism=SCRAM-SHA-1"

  val authConfig: Config = ConfigFactory.parseString(
    s"""
       |akka.contrib.persistence.mongodb.mongo {
       | mongouri = "mongodb://admin:password@$host:$authPort/admin$authMode"
       |}
      """.stripMargin)

  "A secured mongodb instance" should "be connectable via user and pass" in withConfig(authConfig,"akka-contrib-mongodb-persistence-journal","authentication-config") { case (actorSystem, config) =>
    val underTest = new ScalaMongoDriver(actorSystem, config)
    val collections = Await.result(underTest.db.listCollectionNames().toFuture,3.seconds)
    collections should contain ("system.users")
    ()
  }
}