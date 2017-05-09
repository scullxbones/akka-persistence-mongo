package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.testkit.TestKit
import com.mongodb.casbah.Imports._
import org.bson.types.BSONTimestamp

class CasbahTimestampSpec extends TestKit(ActorSystem("unit-test")) with CasbahPersistenceSpec {

  "MongoDB" should "populate timestamp on persist" in withCollection("test-timestamp") { collection =>
    collection.insert(Map("_id" -> "test-1", TIMESTAMP -> new BSONTimestamp))

    val elem = collection.findOneByID("test-1").get
    val ts = elem.as[BSONTimestamp]("ts")

    info(s"Stored ts: $ts")

    ts.getTime should be > 0
  }
}
