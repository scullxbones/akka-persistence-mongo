package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScalaPersistenceJournalTckSpec extends JournalTckSpec(classOf[ScalaDriverPersistenceExtension], s"officialScalaJournalTck")

@RunWith(classOf[JUnitRunner])
class ScalaSuffixPersistenceJournalTckSpec extends JournalTckSpec(classOf[ScalaDriverPersistenceExtension], s"officialScalaJournalTck-suffix", SuffixCollectionNamesTest.extendedConfig)

