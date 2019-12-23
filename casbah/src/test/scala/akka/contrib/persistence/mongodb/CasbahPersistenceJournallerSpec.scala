/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * Gael BREARD (Orange): issue #179 ActorRef serialization
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, ActorSystem, BootstrapSetup, ExtendedActorSystem, ProviderSelection}
import akka.cluster.sharding.ShardCoordinator.Internal.ShardRegionTerminated
import akka.persistence._
import akka.serialization.SerializationExtension
import akka.testkit._
import com.mongodb.casbah.Imports._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.collection.mutable
import scala.concurrent.Await
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceJournallerSpec extends TestKit(ActorSystem("unit-test")) with CasbahPersistenceSpec {

  import JournallingFieldNames._

  import collection.immutable.{Seq => ISeq}

  override def embedDB = "persistence-journaller-casbah"

  implicit val CasbahSerializers = CasbahSerializersExtension(system)
  import CasbahSerializers.Deserializer._
  import CasbahSerializers.Serializer._
  implicit val serialization = SerializationExtension(system)

  implicit class PimpedDBObject(dbo: DBObject) {
    def firstEvent = dbo.as[MongoDBList](EVENTS).as[DBObject](0)
  }

  def replay[A](buffer: mutable.Buffer[A]): A => Unit = (x: A) => {
    buffer += x
    ()
  }

  trait Fixture {
    val underTest = new CasbahPersistenceJournaller(driver) with MongoPersistenceJournalMetrics {
      override def driverName = "casbah"
    }

    val underExtendedTest = new CasbahPersistenceJournaller(extendedDriver) with MongoPersistenceJournalMetrics {
      override def driverName = "casbah"
    }

    val records: List[PersistentRepr] = List(1L, 2L, 3L).map { sq => PersistentRepr(payload = "payload", sequenceNr = sq, persistenceId = "unit-test", manifest = "M") }

    val threeAtoms: List[AtomicWrite] = ((1L to 9L) grouped 3 toList).map(block =>
      AtomicWrite(ISeq(block.map(sn => PersistentRepr(persistenceId = "three-atoms", sequenceNr = sn, payload = "payload")): _*)))

    val pid = "unit-test"
  }

  "A mongo journal implementation" should "serialize an event with a  (issue #179)" in {
    new Fixture {
      val pConfig = ConfigFactory.load().withValue("akka.remote.netty.tcp.port",ConfigValueFactory.fromAnyRef(0))

      val setup1 = BootstrapSetup().withActorRefProvider(ProviderSelection.Cluster).withConfig(pConfig)

      val system1 = ActorSystem("unit-test2", setup1)
      val extendedSystem1: ExtendedActorSystem = system1.asInstanceOf[ExtendedActorSystem]
      val casbahSerializers1 : CasbahSerializers = CasbahSerializersExtension(system1)
      val address1 = extendedSystem1.provider.getDefaultAddress


      val setup2 = BootstrapSetup().withActorRefProvider(ProviderSelection.Remote).withConfig(pConfig)
      val system2 = ActorSystem("unit-test2", setup2)
      val extendedSystem2: ExtendedActorSystem = system2.asInstanceOf[ExtendedActorSystem]
      val casbahSerializers2 = CasbahSerializersExtension(system2)
      val address2 = extendedSystem2.provider.getDefaultAddress

      address1 should not equal(address2)

      def serialize(ref:ActorRef,casbahSerializers: CasbahSerializers): DBObject ={
        val myPayload = Payload[com.mongodb.DBObject](ShardRegionTerminated(ref))(casbahSerializers.serialization,implicitly,casbahSerializers.dt,casbahSerializers.loader)
        val repr = Atom(pid = "pid", from = 1L, to = 1L, events = ISeq(Event(pid = "pid", sn = 1L, payload = myPayload)))
        val serialized = serializeAtom(repr)
        serialized
      }

      def deser(input:DBObject,casbahSerializers: CasbahSerializers): ActorRef ={
        val deserialized = casbahSerializers.Deserializer.deserializeDocument(input.firstEvent)
        deserialized.payload.content.asInstanceOf[ShardRegionTerminated].region
      }


      val refInSys1 = system1.actorOf(TestStubActors.Counter.props,"myActor")
      val ser1InSys1 = serialize(refInSys1,casbahSerializers1)
      system2.stop(refInSys1)
      watch(refInSys1)
      expectTerminated(refInSys1)


      val refDeser1InSys1AfterDead = deser(ser1InSys1,casbahSerializers1)
      refDeser1InSys1AfterDead should be(refInSys1)


      val ser2InSys1 = serialize(refDeser1InSys1AfterDead,casbahSerializers1)
      val refDeser2InSys2 = deser(ser2InSys1,casbahSerializers2)

      refDeser2InSys2.path.address.hasLocalScope should be(false)
      refDeser2InSys2.path.address should be(extendedSystem1.provider.getDefaultAddress)


    }
    ()
  }

  "A mongo journal implementation" should "serialize and deserialize non-confirmable data" in {
    new Fixture {

      val repr = Atom(pid = "pid", from = 1L, to = 1L, events = ISeq(Event(pid = "pid", sn = 1L, payload = "TEST")))

      val serialized = serializeAtom(repr)

      val atom = serialized

      atom.getAs[String](PROCESSOR_ID) shouldBe Some("pid")
      atom.getAs[Long](FROM) shouldBe Some(1L)
      atom.getAs[Long](TO) shouldBe Some(1L)

      val deserialized = deserializeDocument(serialized.firstEvent)

      deserialized.payload shouldBe StringPayload("TEST", Set.empty)
      deserialized.pid should be("pid")
      deserialized.sn should be(1)
      deserialized.manifest shouldBe empty
      deserialized.sender shouldBe empty
    }
    ()
  }

  it should "create an appropriate index" in {
    new Fixture {
      withJournal { journal =>
        driver.journal

        val idx = journal.getIndexInfo.filter(obj => obj("name").equals(driver.journalIndexName)).head
        idx("unique") should ===(true)
        idx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, FROM -> 1, TO -> 1))

        val seqNumIdx = journal.getIndexInfo.filter(obj => obj("name").equals(driver.journalSeqNrIndexName)).head
        seqNumIdx.getAs[Boolean]("unique") shouldBe None
        seqNumIdx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, TO -> -1))
      }
    }
    ()
  }

  it should "create an appropriate suffixed index" in {
    new Fixture {
      withSuffixedJournal(pid) { journal =>
        extendedDriver.journal(pid)

        // should 'retrieve' (and not 'build') the suffixed journal 
        val journalName = extendedDriver.getJournalCollectionName(pid)
        journalName should be("akka_persistence_journal_unit-test-test")
        extendedDriver.db.collectionExists(journalName) should be(true)

        val idx = journal.getIndexInfo.filter(obj => obj("name").equals(extendedDriver.journalIndexName)).head
        idx("unique") should ===(true)
        idx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, FROM -> 1, TO -> 1))

        val seqNumIdx = journal.getIndexInfo.filter(obj => obj("name").equals(extendedDriver.journalSeqNrIndexName)).head
        seqNumIdx.getAs[Boolean]("unique") shouldBe None
        seqNumIdx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, TO -> -1))
      }
    }
    ()
  }

  it should "insert journal records" in {
    new Fixture {
      withJournal { journal =>
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
        event(MANIFEST) should be("M")
      }
    }
    ()
  }

  it should "insert suffixed journal records" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>

        // should 'build' the journal suffixed by persistenceId: "unit-test"
        underExtendedTest.batchAppend(ISeq(AtomicWrite(records)))

        // should 'retrieve' (and not 'build') the suffixed journal 
        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)
        val journal = drv.getJournal("unit-test")

        journal.size should be(1)

        val atom = journal.head

        atom(PROCESSOR_ID) should be("unit-test")
        atom(FROM) should be(1)
        atom(TO) should be(3)
        val event = atom.as[MongoDBList](EVENTS).as[DBObject](0)
        event(PROCESSOR_ID) should be("unit-test")
        event(SEQUENCE_NUMBER) should be(1)
        event(PayloadKey) should be("payload")
        event(MANIFEST) should be("M")
      }
    }
    ()
  }

  it should "hard delete journal entries" in {
    new Fixture {
      withJournal { journal =>
        underTest.batchAppend(ISeq(AtomicWrite(records)))

        underTest.deleteFrom("unit-test", 2L)

        journal.size should be(1)
        val recone = journal.head
        recone(PROCESSOR_ID) should be("unit-test")
        recone(FROM) should be(3)
        recone(TO) should be(3)
        val events = recone.as[MongoDBList](EVENTS)
        events should have size 1
      }
    }
    ()
  }

  it should "hard delete suffixed journal entries" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        underExtendedTest.batchAppend(ISeq(AtomicWrite(records)))

        underExtendedTest.deleteFrom("unit-test", 2L)

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)
        val journal = drv.getJournal("unit-test")

        journal.size should be(1)
        val recone = journal.head
        recone(PROCESSOR_ID) should be("unit-test")
        recone(FROM) should be(3)
        recone(TO) should be(3)
        val events = recone.as[MongoDBList](EVENTS)
        events should have size 1
      }
    }
    ()
  }

  it should "drop suffixed journal when empty" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        underExtendedTest.batchAppend(ISeq(AtomicWrite(records)))

        underExtendedTest.deleteFrom("unit-test", 3L)

        val journalName = drv.getJournalCollectionName("unit-test")
        drv.db.collectionExists(journalName) should be(false)
      }
    }
    ()
  }

  it should "replay journal entries for a single atom" in {
    new Fixture {
      withJournal { journal =>
        underTest.batchAppend(ISeq(AtomicWrite(records)))

        val buf = mutable.Buffer[PersistentRepr]()
        underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
        buf should have size 2
        buf should contain(PersistentRepr(payload = "payload", sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))
      }
    }
    ()
  }

  it should "replay suffixed journal entries for a single atom" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        underExtendedTest.batchAppend(ISeq(AtomicWrite(records)))

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)

        val buf = mutable.Buffer[PersistentRepr]()
        underExtendedTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
        buf should have size 2
        buf should contain(PersistentRepr(payload = "payload", sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))
      }
    }
    ()
  }

  it should "replay journal entries for multiple atoms" in {
    new Fixture {
      withJournal { journal =>
        records.foreach(r => underTest.batchAppend(ISeq(AtomicWrite(r))))

        val buf = mutable.Buffer[PersistentRepr]()
        underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
        buf should have size 2
        buf should contain(PersistentRepr(payload = "payload", sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))
      }
    }
    ()
  }

  it should "replay suffixed journal entries for multiple atoms" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        records.foreach(r => underExtendedTest.batchAppend(ISeq(AtomicWrite(r))))

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)

        val buf = mutable.Buffer[PersistentRepr]()
        underExtendedTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
        buf should have size 2
        buf should contain(PersistentRepr(payload = "payload", sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))
      }
    }
    ()
  }

  it should "replay correctly against multiple atoms - 1 atom case" in {
    new Fixture {
      withJournal { journal =>
        underTest.batchAppend(ISeq(threeAtoms: _*))

        val buf = mutable.Buffer[PersistentRepr]()
        underTest.replayJournal("three-atoms", 2, 3, 10)(replay(buf)).value.get.get
        val expect = (2L to 3L) map (sn => PersistentRepr(payload = "payload", sequenceNr = sn, persistenceId = "three-atoms"))
        buf should have size 2
        buf should contain only (expect: _*)
      }
    }
    ()
  }

  it should "replay correctly suffixed journal against multiple atoms - 1 atom case" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        underExtendedTest.batchAppend(ISeq(threeAtoms: _*))

        val journalName = drv.getJournalCollectionName("three-atoms")
        journalName should be("akka_persistence_journal_three-atoms-test")
        drv.db.collectionExists(journalName) should be(true)

        val buf = mutable.Buffer[PersistentRepr]()
        underExtendedTest.replayJournal("three-atoms", 2, 3, 10)(replay(buf)).value.get.get
        val expect = (2L to 3L) map (sn => PersistentRepr(payload = "payload", sequenceNr = sn, persistenceId = "three-atoms"))
        buf should have size 2
        buf should contain only (expect: _*)
      }
    }
    ()
  }

  it should "replay correctly against multiple atoms - 2 atom case" in {
    new Fixture {
      withJournal { journal =>
        underTest.batchAppend(ISeq(threeAtoms: _*))

        val buf = mutable.Buffer[PersistentRepr]()
        underTest.replayJournal("three-atoms", 5, 8, 10)(replay(buf)).value.get.get
        val expect = (5L to 8L) map (sn => PersistentRepr(payload = "payload", sequenceNr = sn, persistenceId = "three-atoms"))
        buf should have size 4
        buf should contain only (expect: _*)
      }
    }
    ()
  }

  it should "replay correctly suffixed journal against multiple atoms - 2 atom case" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        underExtendedTest.batchAppend(ISeq(threeAtoms: _*))

        val journalName = drv.getJournalCollectionName("three-atoms")
        journalName should be("akka_persistence_journal_three-atoms-test")
        drv.db.collectionExists(journalName) should be(true)

        val buf = mutable.Buffer[PersistentRepr]()
        underExtendedTest.replayJournal("three-atoms", 5, 8, 10)(replay(buf)).value.get.get
        val expect = (5L to 8L) map (sn => PersistentRepr(payload = "payload", sequenceNr = sn, persistenceId = "three-atoms"))
        buf should have size 4
        buf should contain only (expect: _*)
      }
    }
    ()
  }

  it should "replay correctly against multiple atoms - 3 atom case" in {
    new Fixture {
      withJournal { journal =>
        underTest.batchAppend(ISeq(threeAtoms: _*))

        val buf = mutable.Buffer[PersistentRepr]()
        underTest.replayJournal("three-atoms", 3, 8, 10)(replay(buf)).value.get.get
        val expect = (3L to 8L) map (sn => PersistentRepr(payload = "payload", sequenceNr = sn, persistenceId = "three-atoms"))
        buf should have size 6
        buf should contain only (expect: _*)
      }
    }
    ()
  }

  it should "replay correctly suffixed journal against multiple atoms - 3 atom case" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        underExtendedTest.batchAppend(ISeq(threeAtoms: _*))

        val journalName = drv.getJournalCollectionName("three-atoms")
        journalName should be("akka_persistence_journal_three-atoms-test")
        drv.db.collectionExists(journalName) should be(true)

        val buf = mutable.Buffer[PersistentRepr]()
        underExtendedTest.replayJournal("three-atoms", 3, 8, 10)(replay(buf)).value.get.get
        val expect = (3L to 8L) map (sn => PersistentRepr(payload = "payload", sequenceNr = sn, persistenceId = "three-atoms"))
        buf should have size 6
        buf should contain only (expect: _*)
      }
    }
    ()
  }

  it should "have a default sequence nr when journal is empty" in {
    new Fixture {
      withJournal { journal =>
        val result = underTest.maxSequenceNr("unit-test", 5).value.get.get
        result should be(0)
      }
    }
    ()
  }

  it should "have a default sequence nr when suffixed journal is empty" in {
    new Fixture {
      withSuffixedJournal("unit-test") { journal =>
        journal.name should be("akka_persistence_journal_unit-test-test")
        val result = underExtendedTest.maxSequenceNr("unit-test", 5).value.get.get
        result should be(0)
      }
    }
    ()
  }

  it should "calculate the max sequence nr" in {
    new Fixture {
      withJournal { journal =>
        underTest.batchAppend(ISeq(AtomicWrite(records)))

        val result = underTest.maxSequenceNr("unit-test", 2).value.get.get
        result should be(3)
      }
    }
    ()
  }

  it should "calculate the max sequence nr for suffixed journal" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        underExtendedTest.batchAppend(ISeq(AtomicWrite(records)))

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)

        val result = underExtendedTest.maxSequenceNr("unit-test", 2).value.get.get
        result should be(3)
      }
    }
    ()
  }

  it should "support BSON payloads as MongoDBObjects" in {
    new Fixture {
      withJournal { journal =>
        val documents = List(1L, 2L, 3L).map(sn => PersistentRepr(persistenceId = "unit-test", sequenceNr = sn, payload = MongoDBObject("foo" -> "bar", "baz" -> 1)))
        underTest.batchAppend(ISeq(AtomicWrite(documents)))
        val results = journal.find().limit(1)
          .one()
          .as[MongoDBList](EVENTS).collect({ case x: DBObject => x })

        val first = results.head
        first.getAs[String](PROCESSOR_ID) shouldBe Option("unit-test")
        first.getAs[Long](SEQUENCE_NUMBER) shouldBe Option(1)
        first.getAs[String](TYPE) shouldBe Option("bson")
        val payload = first.as[MongoDBObject](PayloadKey)
        payload.getAs[String]("foo") shouldBe Option("bar")
        payload.getAs[Int]("baz") shouldBe Option(1)
      }
    }
    ()
  }

  it should "support BSON payloads as MongoDBObjects in suffixed journal" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        val documents = List(1L, 2L, 3L).map(sn => PersistentRepr(persistenceId = "unit-test", sequenceNr = sn, payload = MongoDBObject("foo" -> "bar", "baz" -> 1)))
        underExtendedTest.batchAppend(ISeq(AtomicWrite(documents)))

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)
        val journal = drv.getJournal("unit-test")

        val results = journal.find().limit(1)
          .one()
          .as[MongoDBList](EVENTS).collect({ case x: DBObject => x })

        val first = results.head
        first.getAs[String](PROCESSOR_ID) shouldBe Option("unit-test")
        first.getAs[Long](SEQUENCE_NUMBER) shouldBe Option(1)
        first.getAs[String](TYPE) shouldBe Option("bson")
        val payload = first.as[MongoDBObject](PayloadKey)
        payload.getAs[String]("foo") shouldBe Option("bar")
        payload.getAs[Int]("baz") shouldBe Option(1)
      }
    }
    ()
  }

  import concurrent.duration._
  it should "support Serializable w/ Manifest payloads like cluster sharding ones" in {
    new Fixture {
      withJournal { journal =>
        val ar = system.deadLetters
        val msg = akka.cluster.sharding.ShardCoordinator.Internal.ShardRegionRegistered(ar)
        val withSerializedObjects = records.map(_.withPayload(msg))
        val result = underTest.batchAppend(ISeq(AtomicWrite(withSerializedObjects)))

        val writeResult = Await.result(result, 5.seconds.dilated)
        writeResult.foreach(wr => wr shouldBe 'success)

        val buf = mutable.Buffer[PersistentRepr]()
        underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
        buf should have size 2
        buf should contain(PersistentRepr(payload = msg, sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))

      }
    }
    ()
  }

  it should "support Serializable w/ Manifest payloads like cluster sharding ones in suffixed journal" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        val ar = system.deadLetters
        val msg = akka.cluster.sharding.ShardCoordinator.Internal.ShardRegionRegistered(ar)
        val withSerializedObjects = records.map(_.withPayload(msg))
        val result = underExtendedTest.batchAppend(ISeq(AtomicWrite(withSerializedObjects)))

        val writeResult = Await.result(result, 5.seconds.dilated)
        writeResult.foreach(wr => wr shouldBe 'success)

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)

        val buf = mutable.Buffer[PersistentRepr]()
        underExtendedTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
        buf should have size 2
        buf should contain(PersistentRepr(payload = msg, sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))

      }
    }
    ()
  }

  it should "support old-school Serializable payloads" in {
    new Fixture {
      withJournal { journal =>
        val msg = system.deadLetters
        val withSerializedObjects = records.map(_.withPayload(msg))
        val result = underTest.batchAppend(ISeq(AtomicWrite(withSerializedObjects)))

        val writeResult = Await.result(result, 5.seconds.dilated)
        writeResult.foreach(wr => wr shouldBe 'success)

        val buf = mutable.Buffer[PersistentRepr]()
        underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
        buf should have size 2
        buf should contain(PersistentRepr(payload = msg, sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))

      }
    }
    ()
  }

  it should "support old-school Serializable payloads in suffixed journal" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        val msg = system.deadLetters
        val withSerializedObjects = records.map(_.withPayload(msg))
        val result = underExtendedTest.batchAppend(ISeq(AtomicWrite(withSerializedObjects)))

        val writeResult = Await.result(result, 5.seconds.dilated)
        writeResult.foreach(wr => wr shouldBe 'success)

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)

        val buf = mutable.Buffer[PersistentRepr]()
        underExtendedTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get
        buf should have size 2
        buf should contain(PersistentRepr(payload = msg, sequenceNr = 2, persistenceId = "unit-test", manifest = "M"))

      }
    }
    ()
  }

  it should "write serializer id in suffixed journal" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        val ar = system.deadLetters
        val msg = akka.cluster.sharding.ShardCoordinator.Internal.ShardRegionRegistered(ar)
        val withSerializedObjects = records.map(_.withPayload(msg))
        val serializer = serialization.serializerFor(msg.getClass)

        val result = underExtendedTest.batchAppend(ISeq(AtomicWrite(withSerializedObjects)))

        val writeResult = Await.result(result, 5.seconds.dilated)
        writeResult.foreach(wr => wr shouldBe 'success)

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)

        val allSerIds = drv.db(journalName).find().flatMap { dbo =>
          dbo.as[MongoDBList](EVENTS).map(_.asInstanceOf[DBObject])
        }.map(_.as[Int](SER_ID))

        allSerIds.toList should contain theSameElementsInOrderAs records.map(_ => serializer.identifier)
      }
    }
    ()
  }

  it should "record metrics" in {
    new Fixture {
      withJournal { journal =>
        underTest.batchAppend(ISeq(AtomicWrite(records)))

        val buf = mutable.Buffer[PersistentRepr]()
        underTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get

        val registry = DropwizardMetrics.metricRegistry
        registry.getTimers() should have size 4
        registry.getTimers().keySet() should contain("akka-persistence-mongo.journal.casbah.read.max-seq.timer")
        registry.getTimers().get("akka-persistence-mongo.journal.casbah.read.max-seq.timer").getCount should be > 0L
      }
    }
    ()
  }

  it should "record metrics for suffixed journal" in {
    new Fixture {
      withAutoSuffixedJournal { drv =>
        underExtendedTest.batchAppend(ISeq(AtomicWrite(records)))

        val journalName = drv.getJournalCollectionName("unit-test")
        journalName should be("akka_persistence_journal_unit-test-test")
        drv.db.collectionExists(journalName) should be(true)

        val buf = mutable.Buffer[PersistentRepr]()
        underExtendedTest.replayJournal("unit-test", 2, 3, 10)(replay(buf)).value.get.get

        val registry = DropwizardMetrics.metricRegistry
        registry.getTimers() should have size 4
        registry.getTimers().keySet() should contain("akka-persistence-mongo.journal.casbah.read.max-seq.timer")
        registry.getTimers().get("akka-persistence-mongo.journal.casbah.read.max-seq.timer").getCount should be > 0L
      }
    }
    ()
  }
}