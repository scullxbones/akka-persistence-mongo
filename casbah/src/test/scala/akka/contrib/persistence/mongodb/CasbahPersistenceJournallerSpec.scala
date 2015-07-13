package akka.contrib.persistence.mongodb

import java.util.{List => JList}

import akka.actor.ActorSystem
import akka.persistence._
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import com.mongodb.casbah.Imports._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceJournallerSpec extends TestKit(ActorSystem("unit-test")) with CasbahPersistenceSpec {

  import collection.immutable.{Seq => ISeq}
  import CasbahSerializers.Deserializer._
  import CasbahSerializers.Serializer._
  import JournallingFieldNames._

  implicit val serialization = SerializationExtension(system)

  trait Fixture {
    val underTest = new CasbahPersistenceJournaller(driver)
    val records:List[PersistentRepr] = List(1, 2, 3).map { sq => PersistentRepr(payload = "payload", sequenceNr = sq, persistenceId = "unit-test") }
  }

  "A mongo journal implementation" should "serialize and deserialize non-confirmable data" in new Fixture {

    val repr = Atom(pid = "pid", from = 1L, to = 1L, events = ISeq(Event(pid = "pid", sn = 1L, payload = "TEST")))

    val serialized = serializeAtom(repr :: Nil)

    serialized(s"$ATOM.$PROCESSOR_ID") should be("pid")
    serialized(s"$ATOM.$FROM") should be(1L)
    serialized(s"$ATOM.$TO") should be(1L)

    val deserialized = deserializeDocument(serialized.as[BasicDBList](ATOM,EVENTS).get(0).asInstanceOf[DBObject])

    deserialized.payload shouldBe StringPayload("TEST")
    deserialized.pid should be("pid")
    deserialized.sn should be(1)
    deserialized.manifest shouldBe empty
    deserialized.sender shouldBe empty
  }

  it should "create an appropriate index" in new Fixture { withJournal { journal =>
    driver.journal

    val idx = journal.getIndexInfo.filter(obj => obj("name").equals(driver.journalIndexName)).head
    idx("unique") should ===(true)
    idx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1, DELETED -> 1))
  }}

  it should "insert journal records" in new Fixture { withJournal { journal =>
    underTest.atomicAppend(AtomicWrite(records))

    journal.size should be(3)

    val recone = journal.head
    recone(PROCESSOR_ID) should be("unit-test")
    recone(SEQUENCE_NUMBER) should be(1)
    recone(DELETED) should ===(false)
    recone(CONFIRMS).asInstanceOf[JList[_]] shouldBe empty
  }}

  it should "hard delete journal entries" in new Fixture { withJournal { journal =>
    underTest.atomicAppend(AtomicWrite(records))

    underTest.deleteFrom("unit-test", 2L)

    journal.size should be(1)
    val recone = journal.head
    recone(PROCESSOR_ID) should be("unit-test")
    recone(SEQUENCE_NUMBER) should be(3)
    recone(DELETED) should ===(false)
  }}
  
  it should "replay journal entries" in new Fixture { withJournal { journal =>
    underTest.atomicAppend(AtomicWrite(records))

    var buf = mutable.Buffer[PersistentRepr]()
    underTest.replayJournal("unit-test", 2, 3, 10)(buf += _).value.get.get
    buf should have size 2
    buf should contain(PersistentRepr(payload = "payload", sequenceNr = 2, persistenceId = "unit-test"))
  }}

  it should "not replay deleted journal entries" in new Fixture { withJournal { journal =>
    underTest.atomicAppend(AtomicWrite(records))

    underTest.deleteFrom("unit-test", 2L)

    var buf = mutable.Buffer[PersistentRepr]()
    underTest.replayJournal("unit-test", 1, 15, 10)(buf += _).value.get.get
    buf should have size 1
    buf should contain(PersistentRepr(payload = "payload", sequenceNr = 3, persistenceId = "unit-test"))
  }}

  it should "have a default sequence nr when journal is empty" in new Fixture { withJournal { journal =>
    val result = underTest.maxSequenceNr("unit-test", 5).value.get.get
    result should be (0)
  }}

  it should "calculate the max sequence nr" in new Fixture { withJournal { journal =>
    underTest.atomicAppend(AtomicWrite(records))

    val result = underTest.maxSequenceNr("unit-test", 2).value.get.get
    result should be (3)
  }}

  it should "support BSON payloads as MongoDBObjects" in new Fixture { withJournal { journal =>
    val documents = List(1,2,3).map(sn => PersistentRepr(persistenceId = "unit-test", sequenceNr = sn, payload = MongoDBObject("foo" -> "bar", "baz" -> 1)))
    underTest.atomicAppend(AtomicWrite(documents))
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