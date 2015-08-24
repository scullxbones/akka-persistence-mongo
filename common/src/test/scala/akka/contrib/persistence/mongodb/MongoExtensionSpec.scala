package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

class MongoExtensionSpec extends BaseUnitTest {

  val driver = ConfigFactory.parseString(
    s"""
      |akka.contrib.persistence.mongodb.mongo.driver="${classOf[StubbyMongoPersistenceExtension].getName}"
    """.stripMargin)

  "A mongo extension" should "load and verify the delivered configuration" in {
    val system = ActorSystem("unit-test",driver)

    try {
      MongoPersistenceExtension.get(system) // Should not throw an exception
      ()
    } finally {
      system.terminate()
      ()
    }
  }

}

class StubbyMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension {
  override def journaler: MongoPersistenceJournallingApi = null

  override def snapshotter: MongoPersistenceSnapshottingApi = null

  override def readJournal: MongoPersistenceReadJournallingApi = null
}

