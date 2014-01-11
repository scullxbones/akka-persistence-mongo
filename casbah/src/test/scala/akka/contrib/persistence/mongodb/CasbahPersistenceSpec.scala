package akka.contrib.persistence.mongodb

import scala.concurrent.ExecutionContext
import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import scala.concurrent.duration._
import scala.language.postfixOps
import com.mongodb.casbah.MongoCollection

trait CasbahPersistenceSpec extends BaseUnitTest with EmbedMongo { self: TestKit =>

    implicit val callerRuns = new ExecutionContext {
      def reportFailure(t: Throwable) { t.printStackTrace() }
      def execute(runnable: Runnable) { runnable.run() }
    }

    val driver = new CasbahPersistenceDriver {
      val actorSystem = system
      override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10 seconds, 10 seconds)
      override def collection(name: String) = mongoDB(name)
    }

    def withCollection(name: String)(testCode: MongoCollection => Any) = {
      val collection = mongoDB(name)
      try {
        testCode(collection)
      } finally {
        collection.dropCollection
      }
    }
    
    def withJournal(testCode: MongoCollection => Any) = 
      withCollection(driver.journalCollectionName)(testCode)

    def withSnapshot(testCode: MongoCollection => Any) =
      withCollection(driver.snapsCollectionName)(testCode)
  
}