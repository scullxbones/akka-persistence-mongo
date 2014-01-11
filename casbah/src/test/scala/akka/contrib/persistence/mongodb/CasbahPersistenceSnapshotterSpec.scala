package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.testkit.TestKit
import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import akka.persistence.SelectedSnapshot
import akka.persistence.SnapshotMetadata
import com.mongodb.casbah.Imports._
import scala.util.Success

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceSnapshotterSpec extends TestKit(ActorSystem("unit-test")) with CasbahPersistenceSpec {

  import CasbahPersistenceSnapshotter._
  import SnapshottingFieldNames._
  import MongoMatchers._

  implicit val serialization = SerializationExtension(system)

  trait Fixture {
    val underTest = new CasbahPersistenceSnapshotter(driver)
    val records = List(10, 20, 30).map { sq =>
      SelectedSnapshot(SnapshotMetadata("unit-test", sq, 10 * sq), "snapshot-data")
    } :+ SelectedSnapshot(SnapshotMetadata("unit-test", 30, 10000), "snapshot-data")
  }

  "A mongo snapshot implementation" should "serialize and deserialize snapshots" in new Fixture {
    val snapshot = records.head
    val serialized = serializeSnapshot(snapshot)
    serialized(PROCESSOR_ID) should be("unit-test")
    serialized(SEQUENCE_NUMBER) should be(10)
    serialized(TIMESTAMP) should be(100)

    val deserialized = deserializeSnapshot(serialized)
    deserialized.metadata.processorId should be("unit-test")
    deserialized.metadata.sequenceNr should be(10)
    deserialized.metadata.timestamp should be(100)
    deserialized.snapshot should be("snapshot-data")
  }

  it should "create an appropriate index" in new Fixture {
    withSnapshot { snapshot =>
      underTest.snaps
      val idx = snapshot.getIndexInfo.filter(obj => obj("name").equals(driver.snapsIndexName)).head
      idx("unique") should ===(true)
      idx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
    }
  }

  it should "find nothing by sequence where time is earlier than first snapshot" in new Fixture {
    withSnapshot { snapshot =>
      snapshot.insert(records: _*)

      underTest.findYoungestSnapshotByMaxSequence("unit-test", 10, 10).value.get.get shouldBe (None)
    }
  }

  it should "find a prior sequence where time is earlier than first snapshot for the max sequence" in new Fixture {
    withSnapshot { snapshot =>
      snapshot.insert(records: _*)

      underTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 250).value.get.get shouldBe
        (Some(SelectedSnapshot(SnapshotMetadata("unit-test", 20, 200), "snapshot-data")))
    }
  }

  it should "find the first snapshot by sequence where time is between the first and second snapshot" in new Fixture {
    withSnapshot { snapshot =>
      snapshot.insert(records: _*)

      underTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 350).value.get.get shouldBe
        (Some(SelectedSnapshot(SnapshotMetadata("unit-test", 30, 300), "snapshot-data")))
    }
  }

  it should "find the last snapshot by sequence where time is after the second snapshot" in new Fixture {
    withSnapshot { snapshot =>
      snapshot.insert(records: _*)

      underTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 25000).value.get.get shouldBe
        (Some(SelectedSnapshot(SnapshotMetadata("unit-test", 30, 10000), "snapshot-data")))
    }
  }

  it should "save a snapshot" in new Fixture {
    withSnapshot { snapshot =>

      snapshot.insert(records: _*)

      underTest.saveSnapshot(SelectedSnapshot(SnapshotMetadata("unit-test", 4, 1000), "snapshot-payload"))

      val saved = snapshot.findOne(MongoDBObject(SEQUENCE_NUMBER -> 4)).get
      saved(PROCESSOR_ID) should be("unit-test")
      saved(SEQUENCE_NUMBER) should be(4)
      saved(TIMESTAMP) should be(1000)
    }
  }

  it should "not delete non-existent snapshots" in new Fixture {
    withSnapshot { snapshot =>

      snapshot.insert(records: _*)

      snapshot.size should be(4)
      underTest.deleteSnapshot("unit-test", 3, 0)
      snapshot.size should be(4)

    }
  }

  it should "only delete the specified snapshot" in new Fixture {
    withSnapshot { snapshot =>

      snapshot.insert(records: _*)

      snapshot.size should be(4)
      underTest.deleteSnapshot("unit-test", 30, 300)
      snapshot.size should be(3)

      val result = snapshot.findOne($and(SEQUENCE_NUMBER $eq 30, TIMESTAMP $eq 10000))
      result should be('defined)
    }
  }

  it should "delete nothing if nothing matches the criteria" in new Fixture {
    withSnapshot { snapshot =>

      snapshot.insert(records: _*)
      snapshot.size should be(4)
      underTest.deleteMatchingSnapshots("unit-test", 10, 50)
      snapshot.size should be(4)

    }
  }

  it should "delete only what matches the criteria" in new Fixture {
    withSnapshot { snapshot =>

      snapshot.insert(records: _*)
      snapshot.size should be(4)

      underTest.deleteMatchingSnapshots("unit-test", 30, 350)
      snapshot.size should be(1)

      snapshot.findOne($and(PROCESSOR_ID $eq "unit-test", SEQUENCE_NUMBER $eq 30, TIMESTAMP $eq 10000)) should be('defined)

    }
  }
}