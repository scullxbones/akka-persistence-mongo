/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

class RxMongoJournalUpgradeSpec
  extends JournalUpgradeSpec(
    classOf[RxMongoPersistenceExtension],
    "rxmongo-upgrade",
    (sys,cfg) => new RxMongoDriver(sys,cfg,new RxMongoDriverProvider(sys)),
    RxMongoConfigTest.rxMongoConfig)