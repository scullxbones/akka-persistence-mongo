/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class RxMongoReadJournalSpec extends ReadJournalSpec(classOf[RxMongoPersistenceExtension], "rxMRJ", RxMongoConfigTest.rxMongoConfig)

class RxMongoSuffixReadJournalSpec extends ReadJournalSpec(classOf[RxMongoPersistenceExtension], "rxMRJ-sfx", SuffixCollectionNamesTest.rxMongoExtendedConfig)
