package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.language.postfixOps
import com.mongodb.casbah.{MongoClient, MongoCollection}

trait CasbahPersistenceSpec extends MongoPersistenceSpec[CasbahMongoDriver,MongoCollection] { self: TestKit =>

    lazy val mongoDB = MongoClient(embedConnectionURL,embedConnectionPort)(embedDB)

    override val driver = new CasbahMongoDriver(system, ConfigFactory.empty()) {
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

    override def withJournal(testCode: MongoCollection => Any) =
      withCollection(driver.journalCollectionName)(testCode)

    override def withSnapshot(testCode: MongoCollection => Any) =
      withCollection(driver.snapsCollectionName)(testCode)
  
}