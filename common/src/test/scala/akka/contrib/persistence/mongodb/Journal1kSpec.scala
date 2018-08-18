/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.tagobjects.Slow

abstract class Journal1kSpec(extensionClass: Class[_], database: String, extendedConfig: String = "|") extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll with ScalaFutures {

  import ConfigLoanFixture._

  override def embedDB = s"1k-test-$database"

  override def afterAll() = cleanup()

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.use-legacy-serialization = true
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://$host:$noAuthPort/$embedDB"
    |akka.contrib.persistence.mongodb.mongo.breaker.timeout.call = 0s
    |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
    |akka-contrib-mongodb-persistence-journal {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoJournal"
    |}
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka-contrib-mongodb-persistence-snapshot {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    $extendedConfig
    |""".stripMargin)

  val id = "123"
  import TestStubActors._
  import Counter._
  import akka.pattern._

  import concurrent.duration._

  "A counter" should "persist the counter to 1000" taggedAs Slow in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "1k-test") { case (as,config) =>
    implicit val askTimeout = Timeout(2.minutes)
    val counter = as.actorOf(Counter.props, id)
    (1 to 1000).foreach(_ => counter ! Inc)
    val result = (counter ? GetCounter).mapTo[Int]
    whenReady(result, timeout(askTimeout.duration + 10.seconds)) { onek =>
      onek shouldBe 1000
    }
  }

  it should "restore the counter back to 1000"  taggedAs Slow in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "1k-test") { case (as, config) =>
    implicit val askTimeout = Timeout(2.minutes)
    val counter = as.actorOf(Counter.props, id)
    val result = (counter ? GetCounter).mapTo[Int]
    whenReady(result, timeout(askTimeout.duration + 10.seconds)) { onek =>
      onek shouldBe 1000
    }
  }
}
