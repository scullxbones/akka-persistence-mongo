/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.testkit.{TestKit, _}
import com.typesafe.config.ConfigFactory
import play.api.libs.iteratee._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{DefaultDB, FailoverStrategy}

import scala.concurrent._
import scala.concurrent.duration._

trait RxMongoPersistenceSpec extends MongoPersistenceSpec[RxMongoDriver, BSONCollection] { self: TestKit =>

  val provider = new RxMongoDriverProvider(system)

  class SpecDriver extends RxMongoDriver(system, ConfigFactory.empty(), provider) {
    override def mongoUri = s"mongodb://$host:$noAuthPort/$embedDB"
  }

  class ExtendedSpecDriver extends RxMongoDriver(system, ConfigFactory.parseString(SuffixCollectionNamesTest.overriddenConfig), provider) {
    override def mongoUri = s"mongodb://$host:$noAuthPort/$embedDB"
  }

  val driver = new SpecDriver
  lazy val specDb: Future[DefaultDB] = driver.db

  val extendedDriver = new ExtendedSpecDriver
  lazy val extendedSpecDb: Future[DefaultDB] = extendedDriver.db

  def withCollection(name: String)(testCode: BSONCollection => Any): Unit = {
    for {
      db <- specDb
      c = db.apply[BSONCollection](name, FailoverStrategy.default)
    } {
      try {
        testCode(c)
        ()
      } finally {
        Await.ready(c.drop(failIfNotFound = false), 3.seconds.dilated)
      }
    }
  }

  def withSuffixedCollection(name: String)(testCode: BSONCollection => Any): Unit = {
    for {
      db <- extendedSpecDb
      c = db[BSONCollection](name)
    } {
      try {
        testCode(c)
        ()
      } finally {
        Await.ready(c.drop(failIfNotFound = false), 3.seconds.dilated)
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

  def withEmptyJournal(testCode: BSONCollection => Any): Unit = withCollection(driver.journalCollectionName) { coll =>
    Await.result(coll.drop(failIfNotFound = false), 3.seconds.dilated)
    testCode(coll)
  }

  def withJournal(testCode: BSONCollection => Any): Unit =
    withCollection(driver.journalCollectionName)(testCode)

  def withSuffixedJournal(pid: String)(testCode: BSONCollection => Any): Unit =
    withSuffixedCollection(extendedDriver.getJournalCollectionName(pid))(testCode)

  def withAutoSuffixedJournal(testCode: RxMongoDriver => Any): Unit =
    withJournalCollections(testCode)

  def withSnapshot(testCode: BSONCollection => Any): Unit =
    withCollection(driver.snapsCollectionName)(testCode)

  def withSuffixedSnapshot(pid: String)(testCode: BSONCollection => Any): Unit =
    withSuffixedCollection(extendedDriver.getSnapsCollectionName(pid))(testCode)

  def withAutoSuffixedSnapshot(testCode: RxMongoDriver => Any): Unit =
    withSnapshotCollections(testCode)

}
