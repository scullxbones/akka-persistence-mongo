package akka.contrib.persistence.mongodb

class CasbahJournalTaggingSpec
  extends JournalTaggingSpec(classOf[CasbahPersistenceExtension], "casbah")

class CasbahSuffixJournalTaggingSpec
  extends JournalTaggingSpec(classOf[CasbahPersistenceExtension], "casbah", SuffixCollectionNamesTest.extendedConfig)
