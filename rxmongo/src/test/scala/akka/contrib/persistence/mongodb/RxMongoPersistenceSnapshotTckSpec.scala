/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RxMongoPersistenceSnapshotTckSpec extends SnapshotTckSpec(classOf[RxMongoPersistenceExtension], "rxMongoSnapshotTck", RxMongoConfigTest.rxMongoConfig)

@RunWith(classOf[JUnitRunner])
class RxMongoSuffixPersistenceSnapshotTckSpec extends SnapshotTckSpec(classOf[RxMongoPersistenceExtension], "rxMongoSuffixSnapshotTck", SuffixCollectionNamesTest.rxMongoExtendedConfig)