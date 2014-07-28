package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.ServerAddress
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverSpec extends BaseUnitTest{

  "A collection of mongo urls" should "be transformed to ServerAddresses" in {
    val mongoUrls = List("localhost:123","localhost:345","localhost:27017")
    val converted = CasbahPersistenceDriver.convertListOfStringsToListOfServerAddresses(mongoUrls)
    converted should have size 3
    converted should equal (List(new ServerAddress("localhost",123), new ServerAddress("localhost",345), new ServerAddress("localhost",27017)))
  }

}

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverAuthSpec extends BaseUnitTest with EmbedMongo {

  override def embedDB = "admin"
  override def auth = new AuthenticatingCommandLinePostProcessor()

  class MockCasbahPersistenceDriver extends CasbahPersistenceDriver {

    val actorSystem = ActorSystem("authentication-spec")

    /**
     *         |akka {
        |  contrib {
        |    persistence {
        |      mongodb {
        |      }
        |    }
        |  }
        |}

     */

    override val userOverrides = ConfigFactory.parseString(
      s"""
        |        mongo {
        |          urls = [ "localhost:$embedConnectionPort" ]
        |          db = "admin"
        |          username = "admin"
        |          password = "password"
        |        }
      """.stripMargin).resolve()

    def users = collection("system.users")
  }

  "A secured mongodb instance" should "be connectable via user and pass" in {
    val underTest = new MockCasbahPersistenceDriver
    val coll = underTest.users
    coll.size should be (1)
    val admin = coll.head
    admin.get("user") should be ("admin")
  }
}