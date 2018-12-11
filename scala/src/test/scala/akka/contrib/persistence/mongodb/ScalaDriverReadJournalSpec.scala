package akka.contrib.persistence.mongodb

class ScalaDriverReadJournalSpec extends ReadJournalSpec(classOf[ScalaDriverPersistenceExtension], "scala-official")

class ScalaDriverSuffixReadJournalSpec extends ReadJournalSpec(classOf[ScalaDriverPersistenceExtension], "scala-official-suffix", SuffixCollectionNamesTest.extendedConfig)
