/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class RxMongoJournalLoadSpec extends JournalLoadSpec(classOf[RxMongoPersistenceExtension],"rxmongo", RxMongoConfigTest.rxMongoConfig)

class RxMongoSuffixJournalLoadSpec extends JournalLoadSpec(classOf[RxMongoPersistenceExtension], "rxmongo", SuffixCollectionNamesTest.rxMongoExtendedConfig)