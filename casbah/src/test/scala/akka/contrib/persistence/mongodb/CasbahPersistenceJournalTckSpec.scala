package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceJournalTckSpec extends JournalTckSpec(classOf[CasbahPersistenceExtension], s"casbahJournalTck")
