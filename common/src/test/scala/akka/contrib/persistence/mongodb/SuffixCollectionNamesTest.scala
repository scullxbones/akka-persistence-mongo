/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class SuffixCollectionNamesTest extends CanSuffixCollectionNames {
  override def getSuffixFromPersistenceId(persistenceId: String): String = persistenceId  
}

object SuffixCollectionNamesTest {
  val extendedConfig = """
    |akka.contrib.persistence.mongodb.mongo.use-suffixed-collection-names = true 
    |akka.contrib.persistence.mongodb.mongo.suffix-builder.class = "akka.contrib.persistence.mongodb.SuffixCollectionNamesTest"
    |""".stripMargin
    
  val rxMongoExtendedConfig = """
    |akka.contrib.persistence.mongodb.mongo.use-suffixed-collection-names = true 
    |akka.contrib.persistence.mongodb.mongo.suffix-builder.class = "akka.contrib.persistence.mongodb.SuffixCollectionNamesTest"
    |akka.contrib.persistence.mongodb.rxmongo.failover.initialDelay = 300ms 
    |akka.contrib.persistence.mongodb.rxmongo.failover.retries = 15
    |akka.contrib.persistence.mongodb.rxmongo.failover.growth = con
    |akka.contrib.persistence.mongodb.rxmongo.failover.factor = 1
    |""".stripMargin
}