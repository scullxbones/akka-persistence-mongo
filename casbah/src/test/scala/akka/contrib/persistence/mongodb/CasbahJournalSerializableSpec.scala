/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class CasbahJournalSerializableSpec extends JournalSerializableSpec(classOf[CasbahPersistenceExtension],"casbah")

class CasbahSuffixJournalSerializableSpec extends JournalSerializableSpec(classOf[CasbahPersistenceExtension],"casbah", SuffixCollectionNamesTest.extendedConfig)
