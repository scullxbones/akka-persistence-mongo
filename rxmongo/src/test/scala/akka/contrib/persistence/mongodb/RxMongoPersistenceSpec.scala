package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.BSONDocument

import scala.concurrent._
import scala.concurrent.duration._

trait RxMongoPersistenceSpec extends MongoPersistenceSpec[RxMongoDriver, BSONCollection] with BeforeAndAfterAll { self: TestKit =>

  override def afterAll() = {
    cleanup()
    super.afterAll()
  }

  class SpecDriver extends RxMongoDriver(system, ConfigFactory.empty()) {
    override def mongoUri = s"mongodb://$host:$noAuthPort/$embedDB"

    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10.seconds, 10.seconds)
  }

  val driver = new SpecDriver
  lazy val specDb = driver.db

  def withCollection(name: String)(testCode: BSONCollection => Any): Unit = {
    val collection = specDb[BSONCollection](name)
    try {
      testCode(collection)
      ()
    } finally {
      Await.ready(collection.drop(),3.seconds)
      ()
    }
  }

  def withEmptyJournal(testCode: BSONCollection => Any) = withCollection(driver.journalCollectionName) { coll =>
    Await.result(coll.remove(BSONDocument.empty),3.seconds)
    testCode(coll)
  }

  def withJournal(testCode: BSONCollection => Any) =
    withCollection(driver.journalCollectionName)(testCode)

  def withSnapshot(testCode: BSONCollection => Any) =
    withCollection(driver.snapsCollectionName)(testCode)

}
