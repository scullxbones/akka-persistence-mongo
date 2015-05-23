package akka.contrib.persistence.mongodb

import java.util.{List => JList}

import akka.actor.ActorSystem
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import com.mongodb.casbah.Imports._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._
import scala.collection.immutable.{Seq => ISeq}
import scala.collection.mutable
import scala.language.postfixOps

case class PersistentConfirmationImpl(persistenceId: String, sequenceNr: Long, channelId: String) extends PersistentConfirmation
case class PersistentIdImpl(processorId: String, sequenceNr: Long) extends PersistentId

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceJournallerSpec extends TestKit(ActorSystem("unit-test")) with CasbahPersistenceSpec {

  import akka.contrib.persistence.mongodb.CasbahPersistenceJournaller._
  import akka.contrib.persistence.mongodb.JournallingFieldNames._

  implicit val serialization = SerializationExtension(system)

  trait Fixture {
    val underTest = new CasbahPersistenceJournaller(driver)
    val records:List[PersistentRepr] = List(1, 2, 3).map { sq => PersistentRepr(payload = "payload", sequenceNr = sq, persistenceId = "unit-test") }
  }

  "A mongo journal implementation" should "serialize and deserialize non-confirmable data" in new Fixture {

    val repr = PersistentRepr(payload = "TEST", sequenceNr = 1, persistenceId = "pid")

    val serialized = serializeJournal(repr)

    serialized(PROCESSOR_ID) should be("pid")
    serialized(DELETED) should ===(false)
    serialized(SEQUENCE_NUMBER) should be(1)

    val deserialized = deserializeJournal(serialized)

    deserialized.payload should be("TEST")
    deserialized.persistenceId should be("pid")
    deserialized.deleted should ===(false)
    deserialized.sequenceNr should be(1)

  }

  it should "serialize and deserialize confirmable data" in new Fixture {
    val repr = PersistentRepr(payload = "TEST", sequenceNr = 1, persistenceId = "pid", confirmable = true, confirms = ISeq("uno"))

    val serialized = serializeJournal(repr)

    serialized(PROCESSOR_ID) should be("pid")
    serialized(DELETED) should ===(false)
    serialized(SEQUENCE_NUMBER) should be(1)

    serialized(CONFIRMS).asInstanceOf[Seq[_]] should have size 1
    serialized(CONFIRMS).asInstanceOf[Seq[_]] should contain("uno")

    val deserialized = deserializeJournal(serialized)

    deserialized.payload should be("TEST")
    deserialized.processorId should be("pid")
    deserialized.deleted should ===(false)
    deserialized.sequenceNr should be(1)
    deserialized.confirms should contain("uno")
  }

  it should "create an appropriate index" in new Fixture { withJournal { journal =>
      underTest.journal

      val idx = journal.getIndexInfo.filter(obj => obj("name").equals(driver.journalIndexName)).head
      idx("unique") should ===(true)
      idx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1, DELETED -> 1))
    }
  }

  it should "insert journal records" in new Fixture { withJournal { journal =>
      underTest.appendToJournal(records)

      journal.size should be(3)

      val recone = journal.head
      recone(PROCESSOR_ID) should be("unit-test")
      recone(SEQUENCE_NUMBER) should be(1)
      recone(DELETED) should ===(false)
      recone(CONFIRMS).asInstanceOf[JList[_]] shouldBe empty
    }
  }

  it should "hard delete journal entries" in new Fixture { withJournal { journal =>
      journal.insert(records: _*)

      underTest.deleteJournalEntries("unit-test", 1, 2, permanent = true)

      journal.size should be(1)
      val recone = journal.head
      recone(PROCESSOR_ID) should be("unit-test")
      recone(SEQUENCE_NUMBER) should be(3)
      recone(DELETED) should ===(false)
    }
  }
  
  it should "hard delete all journal entries by key" in new Fixture { withJournal { journal =>
      journal.insert(records: _*)

      val ids = List(1,2).map { seq => PersistentIdImpl("unit-test",seq) }
      
      underTest.deleteAllMatchingJournalEntries(ids, permanent = true)

      journal.size should be(1)
      val recone = journal.head
      recone(PROCESSOR_ID) should be("unit-test")
      recone(SEQUENCE_NUMBER) should be(3)
      recone(DELETED) should ===(false)
  	}
  }

  it should "soft delete journal entries" in new Fixture { withJournal { journal =>
      journal.insert(records: _*)

      underTest.deleteJournalEntries("unit-test", 1, 2, permanent = false)

      journal.size should be(3)
      val recone = journal.head
      recone(PROCESSOR_ID) should be("unit-test")
      recone(SEQUENCE_NUMBER) should be(1)
      recone(DELETED) should ===(true)

      val rectwo = journal.findOne(MongoDBObject(SEQUENCE_NUMBER -> 2)).get
      rectwo(SEQUENCE_NUMBER) should be(2)
      rectwo(DELETED) should ===(true)

      val recthree = journal.findOne(MongoDBObject(SEQUENCE_NUMBER -> 3)).get
      recthree(SEQUENCE_NUMBER) should be(3)
      recthree(DELETED) should ===(false)
    }
  }

  it should "soft delete all journal entries by key" in new Fixture { withJournal { journal =>
      journal.insert(records: _*)

      val ids = List(1,2).map { seq => PersistentIdImpl("unit-test",seq) }
      
      underTest.deleteAllMatchingJournalEntries(ids, permanent = false)

      journal.size should be(3)
      val recone = journal.head
      recone(PROCESSOR_ID) should be("unit-test")
      recone(SEQUENCE_NUMBER) should be(1)
      recone(DELETED) should ===(true)

      val rectwo = journal.findOne(MongoDBObject(SEQUENCE_NUMBER -> 2)).get
      rectwo(SEQUENCE_NUMBER) should be(2)
      rectwo(DELETED) should ===(true)

      val recthree = journal.findOne(MongoDBObject(SEQUENCE_NUMBER -> 3)).get
      recthree(SEQUENCE_NUMBER) should be(3)
      recthree(DELETED) should ===(false)
    }
  }

  it should "confirm journal entries" in new Fixture { withJournal { journal =>
      journal.insert(records: _*)

      
      val confirms = List(1, 2, 3).flatMap { sq =>
        List(PersistentConfirmationImpl("unit-test", sq, "1chan"),
            PersistentConfirmationImpl("unit-test", sq, "2chan"),
            PersistentConfirmationImpl("unit-test", sq, "3chan"))
      }
      
      underTest.confirmJournalEntries(PersistentConfirmationImpl("unit-test", 1, "4chan") :: confirms)

      val consone :: constwo :: consthree :: _ = List(1, 2, 3).map { sq =>
        journal.findOne(MongoDBObject(SEQUENCE_NUMBER -> sq)).get(CONFIRMS).asInstanceOf[JList[String]].asScala
      }

      consone should contain inOrder ("4chan", "1chan", "2chan", "3chan")
      constwo should contain only ("1chan", "2chan", "3chan")
      consthree should contain only ("1chan", "2chan", "3chan")
    }
  }

  it should "replay journal entries" in new Fixture { withJournal { journal =>
      journal.insert(records: _*)

      var buf = mutable.Buffer[PersistentRepr]()
      val result = underTest.replayJournal("unit-test", 2, 3, 10)(buf += _).value.get.get
      buf should have size 2
      buf should contain(PersistentRepr(payload = "payload", sequenceNr = 2, persistenceId = "unit-test"))
    }
  }

  it should "not replay deleted journal entries" in new Fixture { withJournal { journal =>
      journal.insert(records: _*)

      val ids = List(1,2).map { seq => PersistentIdImpl("unit-test",seq) }
      
      underTest.deleteAllMatchingJournalEntries(ids, permanent = true)
      
      var buf = mutable.Buffer[PersistentRepr]()
      underTest.replayJournal("unit-test", 1, 15, 10)(buf += _).value.get.get
      buf should have size 1
      buf should contain(PersistentRepr(payload = "payload", sequenceNr = 3, persistenceId = "unit-test"))
    }
  }

  it should "have a default sequence nr when journal is empty" in new Fixture { withJournal { journal =>
      val result = underTest.maxSequenceNr("unit-test", 5).value.get.get
      result should be (0)
    }
  }

  it should "calculate the max sequence nr" in new Fixture { withJournal { journal =>
      journal.insert(records: _*)

      val result = underTest.maxSequenceNr("unit-test", 2).value.get.get
      result should be (3)
    }
  }

  it should "support BSON payloads as MongoDBObjects" in new Fixture { withJournal { journal =>
    val documents = List(1,2,3).map(sn => PersistentRepr(persistenceId = "unit-test", sequenceNr = sn, payload = MongoDBObject("foo" -> "bar", "baz" -> 1)))
    underTest.appendToJournal(documents)
    val results = journal.find().toList
    results should have size 3
    val first = results.head
    first.getAs[String](PROCESSOR_ID) shouldBe Some("unit-test")
    first.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(1)
    first.getAs[Boolean](DELETED) shouldBe Some(false)
    val blob = first.getAs[MongoDBObject](SERIALIZED).get
    val payload = blob.getAs[MongoDBObject](PayloadKey).get
    payload.getAs[String]("foo") shouldBe Some("bar")
    payload.getAs[Int]("baz") shouldBe Some(1)
  }}
}