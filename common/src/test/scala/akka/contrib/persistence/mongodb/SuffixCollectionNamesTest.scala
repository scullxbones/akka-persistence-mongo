/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class SuffixCollectionNamesTest extends CanSuffixCollectionNames {
  override def getSuffixFromPersistenceId(persistenceId: String): String = s"$persistenceId-test"  

  override def validateMongoCharacters(input: String): String = {
    // According to mongoDB documentation,
    // forbidden characters in mongoDB collection names (Unix) are /\. "$
    // Forbidden characters in mongoDB collection names (Windows) are /\. "$*<>:|?    
    val forbidden = List('/', '\\', '.', ' ', '\"', '$', '*', '<', '>', ':', '|', '?')

    input.map { c => if (forbidden.contains(c)) '_' else c }
  }
}

object SuffixCollectionNamesTest {
  val extendedConfig = """
    |akka.contrib.persistence.mongodb.mongo.suffix-builder.class = "akka.contrib.persistence.mongodb.SuffixCollectionNamesTest"
    |akka.contrib.persistence.mongodb.mongo.suffix-drop-empty-collections = true
    |""".stripMargin

  def suffixedCollectionName(persistenceId: String): String = {
    val suffix = new SuffixCollectionNamesTest().getSuffixFromPersistenceId(persistenceId)
    s"akka_persistence_journal_$suffix"
  }
    
  val rxMongoExtendedConfig = """
    |akka.contrib.persistence.mongodb.mongo.suffix-builder.class = "akka.contrib.persistence.mongodb.SuffixCollectionNamesTest"
    |akka.contrib.persistence.mongodb.rxmongo.failover.initialDelay = 750ms 
    |akka.contrib.persistence.mongodb.rxmongo.failover.retries = 10
    |akka.contrib.persistence.mongodb.rxmongo.failover.growth = con
    |akka.contrib.persistence.mongodb.rxmongo.failover.factor = 1
    |""".stripMargin
       
  val overriddenConfig = """
    |overrides.suffix-builder.class = "akka.contrib.persistence.mongodb.SuffixCollectionNamesTest"
    |overrides.suffix-drop-empty-collections = true
    |""".stripMargin
}