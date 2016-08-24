/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class CasbahJournalUpgradeSpec extends JournalUpgradeSpec(classOf[CasbahPersistenceExtension], "casbah", new CasbahMongoDriver(_,_))

class CasbahSuffixJournalUpgradeSpec extends JournalUpgradeSpec(classOf[CasbahPersistenceExtension], "casbah", new CasbahMongoDriver(_,_), SuffixCollectionNamesTest.extendedConfig)
