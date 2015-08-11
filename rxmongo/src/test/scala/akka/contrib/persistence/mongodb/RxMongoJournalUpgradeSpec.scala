package akka.contrib.persistence.mongodb

class RxMongoJournalUpgradeSpec extends JournalUpgradeSpec(classOf[RxMongoPersistenceExtension], as => new RxMongoDriver(as))
