package akka.contrib.persistence.mongodb

class ScalaDriverTaggingSpec
  extends JournalTaggingSpec(classOf[ScalaDriverPersistenceExtension], "official-scala-tagging")

class ScalaDriverSuffixTaggingSpec
  extends JournalTaggingSpec(classOf[ScalaDriverPersistenceExtension], "official-scala-tagging-suffix", SuffixCollectionNamesTest.extendedConfig)
