package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import reactivemongo.bson.BSONDocument

import scala.concurrent._
import scala.concurrent.duration._

class RxMongoJournallerSpec extends TestKit(ActorSystem("unit-test")) with RxMongoPersistenceSpec {
  import JournallingFieldNames._

  implicit val serialization = SerializationExtension(system)

  def await[T](block: Future[T])(implicit ec: ExecutionContext) = {
    Await.result(block,3.seconds)
  }

  trait Fixture {
    val underTest = new RxMongoJournaller(driver)
    val records:List[PersistentRepr] = List(1, 2, 3).map { sq =>
      PersistentRepr(payload = "payload", sequenceNr = sq, persistenceId = "unit-test")
    }
    val documents:List[PersistentRepr]  = List(10, 20, 30).map { sq =>
      PersistentRepr(payload = BSONDocument("foo" -> "bar", "baz" -> 1), sequenceNr = sq, persistenceId = "unit-test")
    }
  }

  "A reactive mongo journal implementation" should "insert journal records" in new Fixture { withJournal { journal =>
    val inserted = for {
      inserted <- underTest.atomicAppend(AtomicWrite(records))
      range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
      head <- journal.find(BSONDocument()).cursor().headOption
    } yield (range,head)
    val (range,head) = await(inserted)
    range should have size 3

    val recone = head.get
    recone.getAs[String](PROCESSOR_ID) shouldBe Some("unit-test")
    recone.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(1)
    recone.getAs[Boolean](DELETED) shouldBe Some(false)
  } }

  it should "insert records with documents as payload" in new Fixture { withJournal { journal =>
    val inserted = for {
      inserted <- underTest.atomicAppend(AtomicWrite(documents))
      range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
      head <- journal.find(BSONDocument()).cursor().headOption
    } yield (range,head)
    val (range,head) = await(inserted)
    range should have size 3

    val recone = head.get
    recone.getAs[String](PROCESSOR_ID) shouldBe Some("unit-test")
    recone.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(10)
    recone.getAs[Boolean](DELETED) shouldBe Some(false)
    recone.getAs[BSONDocument](SERIALIZED) foreach { s =>
      s.getAs[BSONDocument](PayloadKey) shouldBe Some(BSONDocument("foo" -> "bar", "baz" -> 1))
    }
  } }

}
