package akka.contrib.persistence.mongodb

class ScalaDriverSerializableSpec extends JournalSerializableSpec(classOf[ScalaDriverPersistenceExtension],"official-scala-ser")

class ScalaDriverSuffixSerializableSpec extends JournalSerializableSpec(classOf[ScalaDriverPersistenceExtension],"official-scala-ser-suffix", SuffixCollectionNamesTest.extendedConfig)

