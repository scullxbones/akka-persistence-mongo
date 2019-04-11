package akka.contrib.persistence.mongodb

class RxMongoJournalTaggingSpec
  extends JournalTaggingSpec(classOf[RxMongoPersistenceExtension], "rxmongo")

class RxMongoSuffixJournalTaggingSpec
  extends JournalTaggingSpec(classOf[RxMongoPersistenceExtension], "rxmongo-suffix", SuffixCollectionNamesTest.rxMongoExtendedConfig)
