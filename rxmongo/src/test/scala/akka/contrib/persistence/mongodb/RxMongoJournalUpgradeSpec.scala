/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class RxMongoJournalUpgradeSpec extends JournalUpgradeSpec(classOf[RxMongoPersistenceExtension], "rxmongo", new RxMongoDriver(_,_), RxMongoConfigTest.rxMongoConfig)

class RxMongoSuffixJournalUpgradeSpec extends JournalUpgradeSpec(classOf[RxMongoPersistenceExtension], "rxmongo", new RxMongoDriver(_,_), SuffixCollectionNamesTest.rxMongoExtendedConfig)
