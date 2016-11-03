/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import akka.testkit._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import reactivemongo.api.collections.bson.BSONCollection
import play.api.libs.iteratee._
import reactivemongo.api.FailoverStrategy

import scala.concurrent._
import scala.concurrent.duration._

trait RxMongoPersistenceSpec extends MongoPersistenceSpec[RxMongoDriver, BSONCollection] { self: TestKit =>

  class SpecDriver extends RxMongoDriver(system, ConfigFactory.empty()) {
    override def mongoUri = s"mongodb://$host:$noAuthPort/$embedDB"

    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10.seconds, 10.seconds)
  }

  class ExtendedSpecDriver extends RxMongoDriver(system, ConfigFactory.parseString(SuffixCollectionNamesTest.overriddenConfig)) {
    override def mongoUri = s"mongodb://$host:$noAuthPort/$embedDB"

    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10.seconds, 10.seconds)
  }

  val driver = new SpecDriver
  lazy val specDb = driver.db

  val extendedDriver = new ExtendedSpecDriver
  lazy val extendedSpecDb = extendedDriver.db

  def withCollection(name: String)(testCode: BSONCollection => Any): Unit = {
    for {
      db <- specDb
      c = db.apply[BSONCollection](name, FailoverStrategy.default)
    } yield {
      try {
        testCode(c)
        ()
      } finally {
        Await.ready(c.drop(failIfNotFound = false), 3.seconds.dilated)
        ()
      }
    }
  }

  def withSuffixedCollection(name: String)(testCode: BSONCollection => Any): Unit = {
    for {
      db <- extendedSpecDb
      c = db[BSONCollection](name)
    } yield {
      try {
        testCode(c)
        ()
      } finally {
        Await.ready(c.drop(failIfNotFound = false), 3.seconds.dilated)
        ()
      }
    }
  }

  def withJournalCollections(testCode: RxMongoDriver => Any): Unit = {
    try {
      testCode(extendedDriver)
      ()
    } finally {
      Await.ready(extendedDriver.getJournalCollections().through(Enumeratee.mapM(coll => coll.drop(failIfNotFound = false))).run(Iteratee.foreach { _ => () }), 3.seconds.dilated)
      Await.ready(extendedDriver.metadata.flatMap(_.drop(failIfNotFound = false)), 3.seconds.dilated)
      ()
    }
  }

  def withSnapshotCollections(testCode: RxMongoDriver => Any): Unit = {
    try {
      testCode(extendedDriver)
      ()
    } finally {
      Await.ready(extendedDriver.getSnapshotCollections().through(Enumeratee.mapM(coll => coll.drop(failIfNotFound = false))).run(Iteratee.foreach { _ => () }), 3.seconds.dilated)
      ()
    }
  }

  def withEmptyJournal(testCode: BSONCollection => Any) = withCollection(driver.journalCollectionName) { coll =>
    Await.result(coll.drop(failIfNotFound = false), 3.seconds.dilated)
    testCode(coll)
  }

  def withJournal(testCode: BSONCollection => Any) =
    withCollection(driver.journalCollectionName)(testCode)

  def withSuffixedJournal(pid: String)(testCode: BSONCollection => Any) =
    withSuffixedCollection(extendedDriver.getJournalCollectionName(pid))(testCode)

  def withAutoSuffixedJournal(testCode: RxMongoDriver => Any) =
    withJournalCollections(testCode)

  def withSnapshot(testCode: BSONCollection => Any) =
    withCollection(driver.snapsCollectionName)(testCode)

  def withSuffixedSnapshot(pid: String)(testCode: BSONCollection => Any) =
    withSuffixedCollection(extendedDriver.getSnapsCollectionName(pid))(testCode)

  def withAutoSuffixedSnapshot(testCode: RxMongoDriver => Any) =
    withSnapshotCollections(testCode)

}
