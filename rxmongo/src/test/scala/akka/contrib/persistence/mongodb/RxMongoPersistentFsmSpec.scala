package akka.contrib.persistence.mongodb

class RxMongoPersistentFsmSpec extends PersistentFsmSpec(classOf[RxMongoPersistenceExtension], "rxmongo-fsm")
