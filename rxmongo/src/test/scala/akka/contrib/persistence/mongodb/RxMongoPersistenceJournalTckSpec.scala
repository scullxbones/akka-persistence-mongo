/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceJournalTckSpec extends JournalTckSpec(classOf[RxMongoPersistenceExtension], "rxMongoJournalTck", RxMongoConfigTest.rxMongoConfig)


@RunWith(classOf[JUnitRunner])
class RxMongoSuffixPersistenceJournalTckSpec extends JournalTckSpec(classOf[RxMongoPersistenceExtension], "rxMongoSuffixJournalTck-suffix", SuffixCollectionNamesTest.rxMongoExtendedConfig)