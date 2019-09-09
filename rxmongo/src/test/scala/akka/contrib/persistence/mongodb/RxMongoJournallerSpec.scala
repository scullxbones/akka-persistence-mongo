/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.SerializationExtension
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit._
import reactivemongo.api.Cursor
import reactivemongo.bson._

import scala.collection.immutable.{Seq => ISeq}
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random

class RxMongoJournallerSpec extends TestKit(ActorSystem("unit-test")) with RxMongoPersistenceSpec {
  import JournallingFieldNames._

  override def embedDB = "persistence-journaller-rxmongo"

  implicit val serialization = SerializationExtension(system)
  implicit val as: ActorSystem = system
  implicit val am: Materializer = ActorMaterializer()

  def await[T](block: Future[T])(implicit ec: ExecutionContext): T = {
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
          _     <- underTest.batchAppend(ISeq(AtomicWrite(records)))
          range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List](maxDocs = 2, err = Cursor.FailOnError[List[BSONDocument]]())
          head  <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        underTest.journalRange("unit-test", 1, 3, Int.MaxValue).runFold(List.empty[Event])(_ :+ _).onComplete {
          case scala.util.Failure(t) => t.printStackTrace()
          case _ => ()
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
        journalName should be("akka_persistence_journal_unit-test-test")

        val inserted: Future[(List[BSONDocument],Option[BSONDocument])] = for {
          // should 'build' the journal suffixed by persistenceId: "unit-test"
          _       <- underExtendedTest.batchAppend(ISeq(AtomicWrite(records)))
          
          // should 'retrieve' (and not 'build') the suffixed journal
          journal <- drv.getJournal("unit-test")

          range   <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List](maxDocs = 2, err = Cursor.FailOnError[List[BSONDocument]]())
          head    <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        underExtendedTest.journalRange("unit-test", 1, 3, Int.MaxValue).runFold(List.empty[Event])(_ :+ _).onComplete {
          case scala.util.Failure(t) => t.printStackTrace()
          case _ => ()
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
          _     <- underTest.batchAppend(ISeq(AtomicWrite(documents)))
          range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List](maxDocs = 2, err = Cursor.FailOnError[List[BSONDocument]]())
          head  <- journal.find(BSONDocument()).cursor().headOption
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

  it should "insert records with serializer id" in {
    new Fixture {
      withJournal { journal =>

        val ar = system.deadLetters
        val msg = akka.cluster.sharding.ShardCoordinator.Internal.ShardRegionRegistered(ar)
        val withSerializedObjects = records.map(_.withPayload(msg))
        val serializer = serialization.serializerFor(msg.getClass)

        val inserted: Future[(List[BSONDocument],Option[BSONDocument])] = for {
          _     <- underTest.batchAppend(ISeq(AtomicWrite(withSerializedObjects)))
          range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List](maxDocs = 2, err = Cursor.FailOnError[List[BSONDocument]]())
          head  <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        val allDocuments = for {
            h   <- head.toList
            ev  <- h.getAs[BSONArray](EVENTS).toList
            doc <-  ev.values.collect { case e: BSONDocument => e }
        } yield doc

        allDocuments.map(_.getAs[Int](SER_ID).get) should contain theSameElementsInOrderAs records.map(_=> serializer.identifier)
        ()
      }
    }
    ()
  }

  it should "insert records with documents as payload in suffixed journal collection" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>

        val persistenceId = math.abs(Random.nextInt(Int.MaxValue)).toString

        val journalName = drv.getJournalCollectionName(persistenceId)
        journalName should be(s"akka_persistence_journal_$persistenceId-test")
        val events = documents.map(_.update(persistenceId = persistenceId))
        
        val inserted: Future[(List[BSONDocument],Option[BSONDocument])] = for {
          _       <- underExtendedTest.batchAppend(ISeq(AtomicWrite(events)))
          journal <- drv.getJournal(persistenceId)
          range   <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List](maxDocs = 2, err = Cursor.FailOnError[List[BSONDocument]]())
          head    <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        val recone = head.get.getAs[BSONArray](EVENTS).toStream.flatMap(_.values.collect {
          case e: BSONDocument => e
        }).head
        recone.getAs[String](PROCESSOR_ID) shouldBe Some(persistenceId)
        recone.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(10)
        recone.getAs[String](TYPE) shouldBe Some("bson")
        recone.getAs[BSONDocument](PayloadKey) shouldBe Some(BSONDocument("foo" -> "bar", "baz" -> 1))
        ()
      }
    }
    ()
  }

  it should "insert records with serializer id in suffixed journal collection" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>

        val persistenceId = math.abs(Random.nextInt(Int.MaxValue)).toString

        val ar = system.deadLetters
        val msg = akka.cluster.sharding.ShardCoordinator.Internal.ShardRegionRegistered(ar)
        val events = records.map(_.update(persistenceId = persistenceId).withPayload(msg))
        val serializer = serialization.serializerFor(msg.getClass)

        val inserted: Future[(List[BSONDocument],Option[BSONDocument])] = for {
          _       <- underExtendedTest.batchAppend(ISeq(AtomicWrite(events)))
          journal <- drv.getJournal(persistenceId)
          range   <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List](maxDocs = 2, err = Cursor.FailOnError[List[BSONDocument]]())
          head    <- journal.find(BSONDocument()).cursor().headOption
        } yield (range, head)
        val (range, head) = await(inserted)
        range should have size 1

        val allDocuments = head.get.getAs[BSONArray](EVENTS).toStream.flatMap(_.values.collect {
          case e: BSONDocument => e
        })

        allDocuments.map(_.getAs[Int](SER_ID).get).toList should contain theSameElementsInOrderAs records.map(_=> serializer.identifier)
        ()
      }
    }
    ()
  }

}
