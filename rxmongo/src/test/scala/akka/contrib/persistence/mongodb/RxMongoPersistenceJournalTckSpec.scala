package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceJournalTckSpec extends JournalTckSpec with EmbeddedMongo {
  override def extensionClass = classOf[RxMongoPersistenceExtension]
}

