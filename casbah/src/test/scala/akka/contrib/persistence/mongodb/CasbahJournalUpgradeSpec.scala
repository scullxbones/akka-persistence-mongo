package akka.contrib.persistence.mongodb

class CasbahJournalUpgradeSpec extends JournalUpgradeSpec(classOf[CasbahPersistenceExtension], new CasbahMongoDriver(_,_))
