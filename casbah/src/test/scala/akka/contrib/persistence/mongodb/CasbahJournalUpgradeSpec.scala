package akka.contrib.persistence.mongodb

class CasbahJournalUpgradeSpec extends JournalUpgradeSpec(classOf[CasbahPersistenceExtension], "casbah", new CasbahMongoDriver(_,_))
