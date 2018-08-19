/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class CasbahJournalLoadSpec extends JournalLoadSpec(classOf[CasbahPersistenceExtension],"casbah-load")

//class CasbahSuffixJournalLoadSpec extends JournalLoadSpec(classOf[CasbahPersistenceExtension],"casbah", SuffixCollectionNamesTest.extendedConfig)
