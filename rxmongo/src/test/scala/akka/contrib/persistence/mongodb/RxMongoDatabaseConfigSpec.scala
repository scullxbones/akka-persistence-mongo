package akka.contrib.persistence.mongodb

import akka.contrib.persistence.mongodb.ConfigLoanFixture._
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner

/**
  * RxMongo database config spec
  * @author  wanglei
  * @since   15-11-28
  * @version 1.0
  */
@RunWith(classOf[JUnitRunner])
class RxMongoDatabaseConfigSpec extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    doBefore()
  }

  override def afterAll(): Unit = {
    doAfter()
  }

  val config = ConfigFactory.parseString(
    s"""|akka.contrib.persistence.mongodb.mongo {
        | mongouri = "mongodb://localhost:$embedConnectionPort/$embedDB"
        | database = "User-Specified-Database"
        |}
      """.stripMargin)

  override def embedDB = "admin"

  "Persistence store database config" should "be User-Specified-Database" in withConfig(config, "akka-contrib-mongodb-persistence-journal") { case (actorSystem, c) =>
    val underTest = new RxMongoDriver(actorSystem, c)
    assertResult("User-Specified-Database")(underTest.db.name)
    ()
  }
}
