/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class RxMongoJournal1kSpec extends Journal1kSpec(classOf[RxMongoPersistenceExtension],"rxMongoJournal1k", RxMongoConfigTest.rxMongoConfig)

class RxMongoSuffixJournal1kSpec extends Journal1kSpec(classOf[RxMongoPersistenceExtension], "rxMongoSuffixJournal1k", SuffixCollectionNamesTest.rxMongoExtendedConfig)