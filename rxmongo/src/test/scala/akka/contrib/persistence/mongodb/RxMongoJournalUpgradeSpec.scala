package akka.contrib.persistence.mongodb

class RxMongoJournalUpgradeSpec extends JournalUpgradeSpec(classOf[RxMongoPersistenceExtension], "rxmongo", new RxMongoDriver(_,_))
