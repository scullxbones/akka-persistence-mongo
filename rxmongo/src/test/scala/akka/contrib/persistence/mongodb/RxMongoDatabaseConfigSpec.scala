package akka.contrib.persistence.mongodb

import akka.contrib.persistence.mongodb.ConfigLoanFixture._
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration._

/**
  * RxMongo database config spec
  * @author  wanglei
  * @since   15-11-28
  * @version 1.0
  */
@RunWith(classOf[JUnitRunner])
class RxMongoDatabaseConfigSpec extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll with ScalaFutures {

  import scala.concurrent.ExecutionContext.Implicits.global

  override def afterAll(): Unit = {
    cleanup()
  }

  val config: Config = ConfigFactory.parseString(
    s"""|akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://$host:$noAuthPort/$embedDB"
        | database = "User-Specified-Database"
        |}
      """.stripMargin)

  override def embedDB = "admin"

  "Persistence store database config" should "be User-Specified-Database" in withConfig(config, "akka-contrib-mongodb-persistence-journal") { case (actorSystem, c) =>
    val underTest = new RxMongoDriver(actorSystem, c, new RxMongoDriverProvider(actorSystem))
    whenReady(underTest.db, PatienceConfiguration.Timeout(5.seconds))(_.name shouldBe "User-Specified-Database")
  }
}
