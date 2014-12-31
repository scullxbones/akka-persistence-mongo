package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import reactivemongo.api.MongoDriver
import reactivemongo.api.collections.default.BSONCollection

import scala.concurrent._
import duration._

trait RxMongoPersistenceSpec extends BaseUnitTest with EmbeddedMongo { self: TestKit =>

  implicit val callerRuns = new ExecutionContext {
    def reportFailure(t: Throwable) { t.printStackTrace() }
    def execute(runnable: Runnable) { runnable.run() }
  }

  lazy val db = MongoDriver(system).connection(s"$embedConnectionURL:$embedConnectionPort" :: Nil)(embedDB)

  val driver = new RxMongoPersistenceDriver {
    val actorSystem = system
    override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10.seconds, 10.seconds)
    override def collection(name: String) = db(name)
  }

  def withCollection(name: String)(testCode: BSONCollection => Any) = {
    val collection = db[BSONCollection](name)
    try {
      testCode(collection)
    } finally {
      collection.drop()
    }
  }

  def withJournal(testCode: BSONCollection => Any) =
    withCollection(driver.journalCollectionName)(testCode)

  def withSnapshot(testCode: BSONCollection => Any) =
    withCollection(driver.snapsCollectionName)(testCode)

}
