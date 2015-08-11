package akka.contrib.persistence.mongodb

class CasbahJournalUpgradeSpec extends JournalUpgradeSpec(classOf[CasbahPersistenceExtension], as => new CasbahMongoDriver(as))
