package akka.contrib.persistence.mongodb

import com.mongodb.ServerAddress
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceDriverSpec extends BaseUnitTest {

  "A collection of mongo urls" should "be transformed to ServerAddresses" in {
    val mongoUrls = List("localhost:123","localhost:345","localhost:27017")
    val converted = CasbahPersistenceDriver.convertListOfStringsToListOfServerAddresses(mongoUrls)
    converted should have size 3
    converted should equal (List(new ServerAddress("localhost",123), new ServerAddress("localhost",345), new ServerAddress("localhost",27017)))
  }
  
}