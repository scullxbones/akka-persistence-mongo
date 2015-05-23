package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import reactivemongo.api.MongoDriver
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.BSONDocument

import scala.concurrent._
import duration._

trait RxMongoPersistenceSpec extends BaseUnitTest with EmbeddedMongo { self: TestKit =>

  implicit val callerRuns = new ExecutionContext {
    def reportFailure(t: Throwable) { t.printStackTrace() }
    def execute(runnable: Runnable) { runnable.run() }
  }

  lazy val connection = MongoDriver(system).connection(s"$embedConnectionURL:$embedConnectionPort" :: Nil)
  lazy val specDb = connection(embedDB)

  class SpecDriver extends RxMongoPersistenceDriver {
    val actorSystem = system
    override lazy val db = specDb
    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10.seconds, 10.seconds)
    override def collection(name: String) = specDb(name)
    override def mongoDbName = embedDB
  }

  val driver = new SpecDriver

  def withCollection(name: String)(testCode: BSONCollection => Any) = {
    val collection = specDb[BSONCollection](name)
    try {
      testCode(collection)
    } finally {
      Await.ready(collection.drop(),3.seconds)
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
