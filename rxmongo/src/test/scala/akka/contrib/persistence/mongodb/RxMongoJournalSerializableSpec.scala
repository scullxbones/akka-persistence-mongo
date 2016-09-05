/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class RxMongoJournalSerializableSpec extends JournalSerializableSpec(classOf[RxMongoPersistenceExtension],"rxmongo", RxMongoConfigTest.rxMongoConfig)

class RxMongoSuffixJournalSerializableSpec extends JournalSerializableSpec(classOf[RxMongoPersistenceExtension],"rxmongo", SuffixCollectionNamesTest.rxMongoExtendedConfig)
