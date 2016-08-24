/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class CasbahReadJournalSpec extends ReadJournalSpec(classOf[CasbahPersistenceExtension], "casbah")

class CasbahSuffixReadJournalSpec extends ReadJournalSpec(classOf[CasbahPersistenceExtension], "casbah", SuffixCollectionNamesTest.extendedConfig)
