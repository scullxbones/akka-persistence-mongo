package akka.contrib.persistence.mongodb

object RxMongoConfigTest {
  val rxMongoConfig = """
    |akka.contrib.persistence.mongodb.rxmongo.failover.initialDelay = 750ms 
    |akka.contrib.persistence.mongodb.rxmongo.failover.retries = 10
    |akka.contrib.persistence.mongodb.rxmongo.failover.growth = con
    |akka.contrib.persistence.mongodb.rxmongo.failover.factor = 1
    |""".stripMargin
  
}