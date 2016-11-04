/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import scala.concurrent.duration._
import scala.language.postfixOps
import com.mongodb.casbah.{ MongoClient, MongoCollection }

trait CasbahPersistenceSpec extends MongoPersistenceSpec[CasbahMongoDriver, MongoCollection] { self: TestKit =>

  lazy val mongoDB = MongoClient(host, noAuthPort)(embedDB)

  override val driver = new CasbahMongoDriver(system, ConfigFactory.empty()) {
    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10 seconds, 10 seconds)
    override def collection(name: String) = mongoDB(name)
    override lazy val db = mongoDB
  }

  override val extendedDriver = new CasbahMongoDriver(system, ConfigFactory.parseString(SuffixCollectionNamesTest.overriddenConfig)) {
    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10 seconds, 10 seconds)
    override def collection(name: String) = mongoDB(name)
    override lazy val db = mongoDB
  }

  override def withCollection(name: String)(testCode: MongoCollection => Any) = {
    val collection = mongoDB(name)
    try {
      testCode(collection)
    } finally {
      collection.dropCollection()
    }
  }

  override def withJournalCollections(testCode: CasbahMongoDriver => Any) = {
    try {
      testCode(extendedDriver)
      ()
    } finally {
      extendedDriver.getJournalCollections().foreach(_.dropCollection())
      extendedDriver.metadata.dropCollection()
    }
  }

  override def withSnapshotCollections(testCode: CasbahMongoDriver => Any) = {
    try {
      testCode(extendedDriver)
      ()
    } finally {
      extendedDriver.getSnapshotCollections().foreach(_.dropCollection())
    }
  }

  override def withJournal(testCode: MongoCollection => Any) =
    withCollection(driver.journalCollectionName)(testCode)

  override def withSuffixedJournal(pid: String)(testCode: MongoCollection => Any) =
    withCollection(extendedDriver.getJournalCollectionName(pid))(testCode)

  override def withAutoSuffixedJournal(testCode: CasbahMongoDriver => Any) =
    withJournalCollections(testCode)

  override def withSnapshot(testCode: MongoCollection => Any) =
    withCollection(driver.snapsCollectionName)(testCode)

  override def withSuffixedSnapshot(pid: String)(testCode: MongoCollection => Any) =
    withCollection(extendedDriver.getSnapsCollectionName(pid))(testCode)

  override def withAutoSuffixedSnapshot(testCode: CasbahMongoDriver => Any) =
    withSnapshotCollections(testCode)

}