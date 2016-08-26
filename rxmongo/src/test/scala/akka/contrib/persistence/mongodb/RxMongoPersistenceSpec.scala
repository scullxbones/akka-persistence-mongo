/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.BSONDocument
import play.api.libs.iteratee._

import scala.concurrent._
import scala.concurrent.duration._

trait RxMongoPersistenceSpec extends MongoPersistenceSpec[RxMongoDriver, BSONCollection] { self: TestKit =>

  class SpecDriver extends RxMongoDriver(system, ConfigFactory.empty()
    .withValue("akka.contrib.persistence.mongodb.rxmongo.failover.initialDelay", ConfigValueFactory.fromAnyRef("300ms"))
    .withValue("akka.contrib.persistence.mongodb.rxmongo.failover.retries", ConfigValueFactory.fromAnyRef(15))
    .withValue("akka.contrib.persistence.mongodb.rxmongo.failover.growth", ConfigValueFactory.fromAnyRef("con"))
    .withValue("akka.contrib.persistence.mongodb.rxmongo.failover.factor", ConfigValueFactory.fromAnyRef(1))
      ) {
    override def mongoUri = s"mongodb://$host:$noAuthPort/$embedDB"

    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10.seconds, 10.seconds)
  }

  class ExtendedSpecDriver extends RxMongoDriver(system, ConfigFactory.empty()
    .withValue("akka.contrib.persistence.mongodb.rxmongo.failover.initialDelay", ConfigValueFactory.fromAnyRef("300ms"))
    .withValue("akka.contrib.persistence.mongodb.rxmongo.failover.retries", ConfigValueFactory.fromAnyRef(15))
    .withValue("akka.contrib.persistence.mongodb.rxmongo.failover.growth", ConfigValueFactory.fromAnyRef("con"))
    .withValue("akka.contrib.persistence.mongodb.rxmongo.failover.factor", ConfigValueFactory.fromAnyRef(1))
    .withValue("akka.contrib.persistence.mongodb.mongo.use-suffixed-collection-names", ConfigValueFactory.fromAnyRef(true))
    .withValue("akka.contrib.persistence.mongodb.mongo.suffix-builder.class",
      ConfigValueFactory.fromAnyRef("akka.contrib.persistence.mongodb.SuffixCollectionNamesTest"))
    .withValue("akka.contrib.persistence.mongodb.mongo.suffix-builder.separator", ConfigValueFactory.fromAnyRef("_"))) {
    override def mongoUri = s"mongodb://$host:$noAuthPort/$embedDB"

    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10.seconds, 10.seconds)
  }

  val driver = new SpecDriver
  lazy val specDb = driver.db

  val extendedDriver = new ExtendedSpecDriver
  lazy val extendedSpecDb = extendedDriver.db

  def withCollection(name: String)(testCode: BSONCollection => Any): Unit = {
    val collection = specDb[BSONCollection](name)
    try {
      testCode(collection)
      ()
    } finally {
      Await.ready(collection.drop(), 3.seconds)
      ()
    }
  }

  def withSuffixedCollection(name: String)(testCode: BSONCollection => Any): Unit = {
    val collection = extendedSpecDb[BSONCollection](name)
    try {
      testCode(collection)
      ()
    } finally {
      Await.ready(collection.drop(), 3.seconds)
      ()
    }
  }

  def withJournalCollections(testCode: RxMongoDriver => Any): Unit = {
    try {
      testCode(extendedDriver)
      ()
    } finally {
      extendedDriver.getJournalCollections().through(Enumeratee.mapM(coll => coll.drop)).run(Iteratee.foreach { _ => () })
      ()
    }
  }

  def withSnapshotCollections(testCode: RxMongoDriver => Any): Unit = {
    try {
      testCode(extendedDriver)
      ()
    } finally {
      extendedDriver.getSnapshotCollections().through(Enumeratee.mapM(coll => coll.drop)).run(Iteratee.foreach { _ => () })
      ()
    }
  }

  def withEmptyJournal(testCode: BSONCollection => Any) = withCollection(driver.journalCollectionName) { coll =>
    Await.result(coll.remove(BSONDocument.empty), 3.seconds)
    testCode(coll)
  }

  def withJournal(testCode: BSONCollection => Any) =
    withCollection(driver.journalCollectionName)(testCode)

  def withSuffixedJournal(suffix: String)(testCode: BSONCollection => Any) =
    withSuffixedCollection(extendedDriver.getJournalCollectionName(suffix))(testCode)

  def withAutoSuffixedJournal(testCode: RxMongoDriver => Any) =
    withJournalCollections(testCode)

  def withSnapshot(testCode: BSONCollection => Any) =
    withCollection(driver.snapsCollectionName)(testCode)

  def withSuffixedSnapshot(suffix: String)(testCode: BSONCollection => Any) =
    withSuffixedCollection(extendedDriver.getSnapsCollectionName(suffix))(testCode)

  def withAutoSuffixedSnapshot(testCode: RxMongoDriver => Any) =
    withSnapshotCollections(testCode)

}
