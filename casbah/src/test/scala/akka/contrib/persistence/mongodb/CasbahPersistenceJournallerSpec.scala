package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence._
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import com.mongodb.casbah.Imports._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable
import scala.concurrent.Await
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceJournallerSpec extends TestKit(ActorSystem("unit-test")) with CasbahPersistenceSpec {

  import collection.immutable.{Seq => ISeq}
  import CasbahSerializers.Deserializer._
  import CasbahSerializers.Serializer._
  import JournallingFieldNames._

  implicit val serialization = SerializationExtension(system)

  implicit class PimpedDBObject(dbo: DBObject) {
    def firstEvent = dbo.as[MongoDBList](EVENTS).as[DBObject](0)
  }

  def replay[A](buffer: mutable.Buffer[A]): A => Unit = (x:A) => {
    buffer += x
    ()
  }

  trait Fixture {
    val underTest = new CasbahPersistenceJournaller(driver)
    val records:List[PersistentRepr] = List(1L, 2L, 3L).map { sq => PersistentRepr(payload = "payload", sequenceNr = sq, persistenceId = "unit-test", manifest = "M") }
  }

  "A mongo journal implementation" should "serialize and deserialize non-confirmable data" in { new Fixture {

    val repr = Atom(pid = "pid", from = 1L, to = 1L, events = ISeq(Event(pid = "pid", sn = 1L, payload = "TEST")))

    val serialized = serializeAtom(repr)

    val atom = serialized

    atom.getAs[String](PROCESSOR_ID) shouldBe Some("pid")
    atom.getAs[Long](FROM) shouldBe Some(1L)
    atom.getAs[Long](TO) shouldBe Some(1L)

    val deserialized = deserializeDocument(serialized.firstEvent)

    deserialized.payload shouldBe StringPayload("TEST")
    deserialized.pid should be("pid")
    deserialized.sn should be(1)
    deserialized.manifest shouldBe empty
    deserialized.sender shouldBe empty
  }
  () }

  it should "create an appropriate index" in { new Fixture { withJournal { journal =>
    driver.journal

    val idx = journal.getIndexInfo.filter(obj => obj("name").equals(driver.journalIndexName)).head
    idx("unique") should ===(true)
    idx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, FROM -> 1, TO -> 1))
  }}
  () }

  it should "insert journal records" in { new Fixture { withJournal { journal =>
    underTest.batchAppend(ISeq(AtomicWrite(records)))

    journal.size should be(1)

    val atom = journal.head


    atom(PROCESSOR_ID) should be("unit-test")
    atom(FROM) should be(1)
    atom(TO) should be(3)
    val event = atom.as[MongoDBList](EVENTS).as[DBObject](0)
    event(PROCESSOR_ID) should be("unit-test")
    event(SEQUENCE_NUMBER) should be(1)
    event(PayloadKey) should be("payload")
    event(MANIFEST) should be ("M")
  }}
  () }

  it should "hard delete journal entries" in { new Fixture { withJournal { journal =>
    underTest.batchAppend(ISeq(AtomicWrite(records)))

    underTest.deleteFrom("unit-test", 2L)

    journal.size should be(1)
    val recone = journal.head
    recone(PROCESSOR_ID) should be("unit-test")
    recone(FROM) should be(3)
    recone(TO) should be(3)
    val events = recone.as[MongoDBList](EVENTS)
    events should have size 1
  }}
  () }
  
  it should "replay journal entries for a single atom" in { new Fixture { withJournal { journal =>
    underTest.batchAppend(ISeq(AtomicWrite(records)))

    val buf = mutable.Buffer[PersistentRepr]()
    underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
    buf should have size 2
    buf should contain(PersistentRepr(payload = "payload", sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))
  }}
  () }

  it should "replay journal entries for multiple atoms" in { new Fixture { withJournal { journal =>
    records.foreach(r => underTest.batchAppend(ISeq(AtomicWrite(r))))

    val buf = mutable.Buffer[PersistentRepr]()
    underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
    buf should have size 2
    buf should contain(PersistentRepr(payload = "payload", sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))
  }}
  () }

  it should "have a default sequence nr when journal is empty" in { new Fixture { withJournal { journal =>
    val result = underTest.maxSequenceNr("unit-test", 5).value.get.get
    result should be (0)
  }}
  () }

  it should "calculate the max sequence nr" in { new Fixture { withJournal { journal =>
    underTest.batchAppend(ISeq(AtomicWrite(records)))

    val result = underTest.maxSequenceNr("unit-test", 2).value.get.get
    result should be (3)
  }}
  () }

  it should "support BSON payloads as MongoDBObjects" in { new Fixture { withJournal { journal =>
    val documents = List(1L,2L,3L).map(sn => PersistentRepr(persistenceId = "unit-test", sequenceNr = sn, payload = MongoDBObject("foo" -> "bar", "baz" -> 1)))
    underTest.batchAppend(ISeq(AtomicWrite(documents)))
    val results = journal.find().limit(1)
      .one()
      .as[MongoDBList](EVENTS).collect({case x:DBObject => x})

    val first = results.head
    first.getAs[String](PROCESSOR_ID) shouldBe Option("unit-test")
    first.getAs[Long](SEQUENCE_NUMBER) shouldBe Option(1)
    first.getAs[String](TYPE) shouldBe Option("bson")
    val payload = first.as[MongoDBObject](PayloadKey)
    payload.getAs[String]("foo") shouldBe Option("bar")
    payload.getAs[Int]("baz") shouldBe Option(1)
  }}
  () }

  import concurrent.duration._
  it should "support Serializable w/ Manifest payloads like cluster sharding ones" in { new Fixture { withJournal {journal =>
    val ar = system.deadLetters
    val msg = akka.cluster.sharding.ShardCoordinator.Internal.ShardRegionRegistered(ar)
    val withSerializedObjects = records.map(_.withPayload(msg))
    val result =  underTest.batchAppend(ISeq(AtomicWrite(withSerializedObjects)))

    val writeResult = Await.result(result,5.seconds)
    writeResult.foreach(wr => wr shouldBe 'success)

    val buf = mutable.Buffer[PersistentRepr]()
    underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
    buf should have size 2
    buf should contain(PersistentRepr(payload = msg, sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))

  }}
  () }

  it should "support old-school Serializable payloads" in { new Fixture { withJournal {journal =>
    val msg = system.deadLetters
    val withSerializedObjects = records.map(_.withPayload(msg))
    val result =  underTest.batchAppend(ISeq(AtomicWrite(withSerializedObjects)))

    val writeResult = Await.result(result,5.seconds)
    writeResult.foreach(wr => wr shouldBe 'success)

    val buf = mutable.Buffer[PersistentRepr]()
    underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
    buf should have size 2
    buf should contain(PersistentRepr(payload = msg, sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))

  }}
  () }
}