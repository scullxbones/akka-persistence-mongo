package akka.contrib.persistence.mongodb

object RxMongoConfigTest {
  val rxMongoConfig = """
    |akka.contrib.persistence.mongodb.rxmongo.failover.initialDelay = 300ms 
    |akka.contrib.persistence.mongodb.rxmongo.failover.retries = 15
    |akka.contrib.persistence.mongodb.rxmongo.failover.growth = con
    |akka.contrib.persistence.mongodb.rxmongo.failover.factor = 1
    |akka.test.default-timeout = 5 seconds
    |""".stripMargin
  
}