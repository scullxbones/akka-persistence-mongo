package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import scala.concurrent.duration._
import scala.language.postfixOps
import com.mongodb.casbah.{MongoClient, MongoCollection}

trait CasbahPersistenceSpec extends MongoPersistenceSpec[CasbahPersistenceDriver,MongoCollection] { self: TestKit =>

    lazy val mongoDB = MongoClient(embedConnectionURL,embedConnectionPort)(embedDB)

    override val driver = new CasbahPersistenceDriver {
      val actorSystem = system
      override lazy val breaker = CircuitBreaker(system.scheduler, 0, 10 seconds, 10 seconds)
      override def collection(name: String) = mongoDB(name)
    }

    override def withCollection(name: String)(testCode: MongoCollection => Any) = {
      val collection = mongoDB(name)
      try {
        testCode(collection)
      } finally {
        collection.dropCollection()
      }
    }

    override def withJournal(testCode: MongoCollection => Any) =
      withCollection(driver.journalCollectionName)(testCode)

    override def withSnapshot(testCode: MongoCollection => Any) =
      withCollection(driver.snapsCollectionName)(testCode)
  
}