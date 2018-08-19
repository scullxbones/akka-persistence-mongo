package akka.contrib.persistence.mongodb

class CasbahPersistentFsmSpec extends PersistentFsmSpec(classOf[CasbahPersistenceExtension], "casbah-fsm")
