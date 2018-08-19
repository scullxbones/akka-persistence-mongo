/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class CasbahJournal1kSpec extends Journal1kSpec(classOf[CasbahPersistenceExtension], "casbah-1k")

class CasbahSuffixJournal1kSpec extends Journal1kSpec(classOf[CasbahPersistenceExtension], "casbah-1k-suffix", SuffixCollectionNamesTest.extendedConfig)
