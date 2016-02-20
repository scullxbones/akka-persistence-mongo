package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceJournalTckSpec extends JournalTckSpec(classOf[RxMongoPersistenceExtension], "rxMongoJournalTck")

