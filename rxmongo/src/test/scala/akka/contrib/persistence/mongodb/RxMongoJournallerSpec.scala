/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.SerializationExtension
import akka.testkit._
import reactivemongo.bson._
import scala.collection.immutable.{ Seq => ISeq }
import scala.concurrent._
import scala.concurrent.duration._
import play.api.libs.iteratee.Iteratee

class RxMongoJournallerSpec extends TestKit(ActorSystem("unit-test")) with RxMongoPersistenceSpec {
  import JournallingFieldNames._

  override def embedDB = "persistence-journaller-rxmongo"

  implicit val serialization = SerializationExtension(system)
  implicit val as = system

  def await[T](block: Future[T])(implicit ec: ExecutionContext) = {
    Await.result(block, 3.seconds.dilated)
  }

  trait Fixture {
    val underTest = new RxMongoJournaller(driver)

    val underExtendedTest = new RxMongoJournaller(extendedDriver)

    val records: List[PersistentRepr] = List(1L, 2L, 3L).map { sq =>
      PersistentRepr(payload = "payload", sequenceNr = sq, persistenceId = "unit-test")
    }
    val documents: List[PersistentRepr] = List(10L, 20L, 30L).map { sq =>
      PersistentRepr(payload = BSONDocument("foo" -> "bar", "baz" -> 1), sequenceNr = sq, persistenceId = "unit-test")
    }
  }

  "A reactive mongo journal implementation" should "insert journal records" in {
    new Fixture {
      withJournal { journal =>
        val inserted = for {
          inserted <- underTest.batchAppend(ISeq(AtomicWrite(records)))
          range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
          head <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        underTest.journalRange("unit-test", 1, 3, Int.MaxValue).run(Iteratee.getChunks[Event]) onFailure {
          case t => t.printStackTrace()
        }

        val recone = head.get.getAs[BSONArray](EVENTS).toStream.flatMap(_.values.collect {
          case e: BSONDocument => e
        }).head
        recone.getAs[String](PROCESSOR_ID) shouldBe Some("unit-test")
        recone.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(1)

      }
    }
    ()
  }

  it should "insert journal records in suffixed journal collection" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>        
        val journalName = drv.getJournalCollectionName("unit-test")

        val inserted: Future[(List[BSONDocument],Option[BSONDocument])] = for {
          // should 'build' the journal suffixed by persistenceId: "unit-test"
          inserted <- underExtendedTest.batchAppend(ISeq(AtomicWrite(records)))
          
          // should 'retrieve' (and not 'build') the suffixed journal
          db <- drv.db
          collections <- db.collectionNames
          journal <- drv.collection(collections.filter(_.equals(journalName)).head)

          range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
          head <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        underExtendedTest.journalRange("unit-test", 1, 3, Int.MaxValue).run(Iteratee.getChunks[Event]) onFailure {
          case t => t.printStackTrace()
        }

        val recone = head.get.getAs[BSONArray](EVENTS).toStream.flatMap(_.values.collect {
          case e: BSONDocument => e
        }).head
        recone.getAs[String](PROCESSOR_ID) shouldBe Some("unit-test")
        recone.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(1)

      }
    }
    ()
  }

  it should "insert records with documents as payload" in {
    new Fixture {
      withJournal { journal =>
        val inserted = for {
          inserted <- underTest.batchAppend(ISeq(AtomicWrite(documents)))
          range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
          head <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        val recone = head.get.getAs[BSONArray](EVENTS).toStream.flatMap(_.values.collect {
          case e: BSONDocument => e
        }).head
        recone.getAs[String](PROCESSOR_ID) shouldBe Some("unit-test")
        recone.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(10)
        recone.getAs[String](TYPE) shouldBe Some("bson")
        recone.getAs[BSONDocument](PayloadKey) shouldBe Some(BSONDocument("foo" -> "bar", "baz" -> 1))
        ()
      }
    }
    ()
  }

  it should "insert records with documents as payload in suffixed journal collection" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        val journalName = drv.getJournalCollectionName("unit-test")
        
        val inserted: Future[(List[BSONDocument],Option[BSONDocument])] = for {
          inserted <- underExtendedTest.batchAppend(ISeq(AtomicWrite(documents)))
          db <- drv.db
          collections <- db.collectionNames
          journal <- drv.collection(collections.filter(_.equals(journalName)).head)
          range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
          head <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        val recone = head.get.getAs[BSONArray](EVENTS).toStream.flatMap(_.values.collect {
          case e: BSONDocument => e
        }).head
        recone.getAs[String](PROCESSOR_ID) shouldBe Some("unit-test")
        recone.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(10)
        recone.getAs[String](TYPE) shouldBe Some("bson")
        recone.getAs[BSONDocument](PayloadKey) shouldBe Some(BSONDocument("foo" -> "bar", "baz" -> 1))
        ()
      }
    }
    ()
  }

}
