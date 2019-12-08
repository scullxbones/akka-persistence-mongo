package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.ConfigLoanFixture._
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith

import scala.concurrent.Await
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverShutdownSpec extends BaseUnitTest with ContainerMongo {

  val shutdownConfig: Config = ConfigFactory.parseString(
    s"""|akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://$host:$noAuthPort/shutdown-spec"
        | db = "shutdown-spec"
        |}
      """.stripMargin)

  "A casbah driver" should "close the mongodb connection pool on actor system shutdown" in withConfig(shutdownConfig, "akka-contrib-mongodb-persistence-journal") { case (actorSystem,config) =>
    val underTest = new CasbahMongoDriver(actorSystem, config)
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated,10.seconds)
    intercept[IllegalStateException] {
      underTest.db.stats()
    }
    ()
  }


  it should "reconnect if a new driver is created" in withConfig(shutdownConfig, "akka-contrib-mongodb-persistence-journal")  { case (actorSystem,config) =>
    val underTest = new CasbahMongoDriver(actorSystem, config)
    underTest.db.collectionNames()
    underTest.actorSystem.terminate()
    Await.result(underTest.actorSystem.whenTerminated,10.seconds)

    val newAs:ActorSystem = ActorSystem("test2",shutdownConfig)
    val underTest2 = new CasbahMongoDriver(newAs, config)
    underTest2.db.collectionNames()
    newAs.terminate()
    ()
  }
}

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverAuthSpec extends BaseUnitTest with ContainerMongo {

  val authConfig: Config = ConfigFactory.parseString(
    s"""
    |akka.contrib.persistence.mongodb.mongo {
    | mongouri = "mongodb://admin:password@$host:$authPort/admin"
    |}
     """.stripMargin)

  "A secured mongodb instance" should "be connectable via user and pass" in withConfig(authConfig, "akka-contrib-mongodb-persistence-journal") { case (actorSystem,config) =>
    val underTest = new CasbahMongoDriver(actorSystem, config)
    val collections = underTest.db.collectionNames()
    collections should contain ("system.users")
    ()
  }
}
