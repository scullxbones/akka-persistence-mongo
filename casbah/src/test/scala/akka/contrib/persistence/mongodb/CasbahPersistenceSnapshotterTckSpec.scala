/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceSnapshotterTckSpec extends SnapshotTckSpec(classOf[CasbahPersistenceExtension], "casbah-snaptck")


@RunWith(classOf[JUnitRunner])
class CasbahSuffixPersistenceSnapshotterTckSpec extends SnapshotTckSpec(classOf[CasbahPersistenceExtension], "casbah-snaptck-suffix", SuffixCollectionNamesTest.extendedConfig)