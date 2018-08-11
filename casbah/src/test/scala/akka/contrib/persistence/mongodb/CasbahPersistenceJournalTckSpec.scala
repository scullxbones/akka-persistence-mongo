/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceJournalTckSpec extends JournalTckSpec(classOf[CasbahPersistenceExtension], s"casbahJournalTck")

@RunWith(classOf[JUnitRunner])
class CasbahSuffixPersistenceJournalTckSpec extends JournalTckSpec(classOf[CasbahPersistenceExtension], s"casbahJournalTck-suffix", SuffixCollectionNamesTest.extendedConfig)
