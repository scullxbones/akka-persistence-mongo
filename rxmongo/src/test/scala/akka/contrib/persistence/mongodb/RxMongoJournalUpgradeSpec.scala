package akka.contrib.persistence.mongodb

class RxMongoJournalUpgradeSpec extends JournalUpgradeSpec(classOf[RxMongoPersistenceExtension], new RxMongoDriver(_,_))
