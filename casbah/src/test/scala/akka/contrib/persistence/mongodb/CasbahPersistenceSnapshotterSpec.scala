/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata }
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import com.mongodb.casbah.Imports._
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceSnapshotterSpec extends TestKit(ActorSystem("unit-test")) with CasbahPersistenceSpec {

  import akka.contrib.persistence.mongodb.CasbahPersistenceSnapshotter._
  import akka.contrib.persistence.mongodb.SnapshottingFieldNames._

  override def embedDB = "persistence-snapshotter-casbah"

  implicit val serialization = SerializationExtension(system)

  trait Fixture {
    val underTest = new CasbahPersistenceSnapshotter(driver)
    val underExtendedTest = new CasbahPersistenceSnapshotter(extendedDriver)
    val records = List(10L, 20L, 30L).map { sq =>
      SelectedSnapshot(SnapshotMetadata("unit-test", sq, 10L * sq), "snapshot-data")
    } :+ SelectedSnapshot(SnapshotMetadata("unit-test", 30L, 10000L), "snapshot-data")

    val pid = "unit-test"
  }

  "A mongo snapshot implementation" should "serialize and deserialize snapshots" in {
    new Fixture {
      val snapshot = records.head
      val serialized = serializeSnapshot(snapshot)
      serialized(PROCESSOR_ID) should be("unit-test")
      serialized(SEQUENCE_NUMBER) should be(10)
      serialized(TIMESTAMP) should be(100)

      val deserialized = deserializeSnapshot(serialized)
      deserialized.metadata.persistenceId should be("unit-test")
      deserialized.metadata.sequenceNr should be(10)
      deserialized.metadata.timestamp should be(100)
      deserialized.snapshot should be("snapshot-data")
    }
    ()
  }

  it should "create an appropriate index" in {
    new Fixture {
      withSnapshot { snapshot =>
        driver.snaps
        val idx = snapshot.getIndexInfo.filter(obj => obj("name").equals(driver.snapsIndexName)).head
        idx("unique") should ===(true)
        idx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
      }
    }
    ()
  }

  it should "create an appropriate suffixed index" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>
        extendedDriver.snaps(pid)

        // should 'retrieve' (and not 'build') the suffixed snapshot 
        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)

        val idx = snapshot.getIndexInfo.filter(obj => obj("name").equals(driver.snapsIndexName)).head
        idx("unique") should ===(true)
        idx("key") should be(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
      }
    }
    ()
  }

  it should "find nothing by sequence where time is earlier than first snapshot" in {
    new Fixture {
      withSnapshot { snapshot =>
        snapshot.insert(records: _*)

        underTest.findYoungestSnapshotByMaxSequence("unit-test", 10, 10).value.get.get shouldBe None
      }
    }
    ()
  }

  it should "find nothing by sequence where time is earlier than first suffixed snapshot" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>
        snapshot.insert(records: _*)

        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)

        underExtendedTest.findYoungestSnapshotByMaxSequence("unit-test", 10, 10).value.get.get shouldBe None
      }
    }
    ()
  }

  it should "find a prior sequence where time is earlier than first snapshot for the max sequence" in {
    new Fixture {
      withSnapshot { snapshot =>
        snapshot.insert(records: _*)

        underTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 250).value.get.get shouldBe
          Some(SelectedSnapshot(SnapshotMetadata("unit-test", 20, 200), "snapshot-data"))
      }
    }
    ()
  }

  it should "find a prior sequence where time is earlier than first suffixed snapshot for the max sequence" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>
        snapshot.insert(records: _*)

        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)

        underExtendedTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 250).value.get.get shouldBe
          Some(SelectedSnapshot(SnapshotMetadata("unit-test", 20, 200), "snapshot-data"))
      }
    }
    ()
  }

  it should "find the first snapshot by sequence where time is between the first and second snapshot" in {
    new Fixture {
      withSnapshot { snapshot =>
        snapshot.insert(records: _*)

        underTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 350).value.get.get shouldBe
          Some(SelectedSnapshot(SnapshotMetadata("unit-test", 30, 300), "snapshot-data"))
      }
    }
    ()
  }

  it should "find the first snapshot by sequence where time is between the first and second suffixed snapshot" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>
        snapshot.insert(records: _*)

        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)

        underExtendedTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 350).value.get.get shouldBe
          Some(SelectedSnapshot(SnapshotMetadata("unit-test", 30, 300), "snapshot-data"))
      }
    }
    ()
  }

  it should "find the last snapshot by sequence where time is after the second snapshot" in {
    new Fixture {
      withSnapshot { snapshot =>
        snapshot.insert(records: _*)

        underTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 25000).value.get.get shouldBe
          Some(SelectedSnapshot(SnapshotMetadata("unit-test", 30, 10000), "snapshot-data"))
      }
    }
    ()
  }

  it should "find the last snapshot by sequence where time is after the second suffixed snapshot" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>
        snapshot.insert(records: _*)

        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)

        underExtendedTest.findYoungestSnapshotByMaxSequence("unit-test", 30, 25000).value.get.get shouldBe
          Some(SelectedSnapshot(SnapshotMetadata("unit-test", 30, 10000), "snapshot-data"))
      }
    }
    ()
  }

  it should "save a snapshot" in {
    new Fixture {
      withSnapshot { snapshot =>

        underTest.saveSnapshot(SelectedSnapshot(SnapshotMetadata("unit-test", 4, 1000), "snapshot-payload"))

        val saved = snapshot.findOne(MongoDBObject(SEQUENCE_NUMBER -> 4)).get
        saved(PROCESSOR_ID) should be("unit-test")
        saved(SEQUENCE_NUMBER) should be(4)
        saved(TIMESTAMP) should be(1000)
      }
    }
    ()
  }

  it should "save a suffixed snapshot" in {
    new Fixture {
      withAutoSuffixedSnapshot { drv =>

        underExtendedTest.saveSnapshot(SelectedSnapshot(SnapshotMetadata("unit-test", 4, 1000), "snapshot-payload"))
        
        val snapsName = drv.getSnapsCollectionName("unit-test")
        snapsName should be("akka_persistence_snaps_unit-test-test")
        drv.db.collectionExists(snapsName) should be(true)
        val snapshot = drv.collection(snapsName)

        val saved = snapshot.findOne(MongoDBObject(SEQUENCE_NUMBER -> 4)).get
        saved(PROCESSOR_ID) should be("unit-test")
        saved(SEQUENCE_NUMBER) should be(4)
        saved(TIMESTAMP) should be(1000)
      }
    }
    ()
  }

  it should "not delete non-existent snapshots" in {
    new Fixture {
      withSnapshot { snapshot =>

        snapshot.insert(records: _*)

        snapshot.size should be(4)
        underTest.deleteSnapshot("unit-test", 3, 0)
        snapshot.size should be(4)

      }
    }
    ()
  }

  it should "not delete non-existent suffixed snapshots" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>

        snapshot.insert(records: _*)
        
        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)

        snapshot.size should be(4)
        underExtendedTest.deleteSnapshot("unit-test", 3, 0)
        snapshot.size should be(4)

      }
    }
    ()
  }

  it should "only delete the specified snapshot" in {
    new Fixture {
      withSnapshot { snapshot =>

        snapshot.insert(records: _*)

        snapshot.size should be(4)
        underTest.deleteSnapshot("unit-test", 30, 300)
        snapshot.size should be(3)

        val result = snapshot.findOne($and(SEQUENCE_NUMBER $eq 30, TIMESTAMP $eq 10000))
        result should be('defined)
      }
    }
    ()
  }

  it should "only delete the specified suffixed snapshot" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>

        snapshot.insert(records: _*)
        
        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)

        snapshot.size should be(4)
        underExtendedTest.deleteSnapshot("unit-test", 30, 300)
        snapshot.size should be(3)

        val result = snapshot.findOne($and(SEQUENCE_NUMBER $eq 30, TIMESTAMP $eq 10000))
        result should be('defined)
      }
    }
    ()
  }

  it should "delete nothing if nothing matches the criteria" in {
    new Fixture {
      withSnapshot { snapshot =>

        snapshot.insert(records: _*)
        snapshot.size should be(4)
        underTest.deleteMatchingSnapshots("unit-test", 10, 50)
        snapshot.size should be(4)

      }
    }
    ()
  }

  it should "delete nothing if nothing matches the criteria in suffixed snapshot" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>

        snapshot.insert(records: _*)
        
        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)
        
        snapshot.size should be(4)
        
        underExtendedTest.deleteMatchingSnapshots("unit-test", 10, 50)
        snapshot.size should be(4)

      }
    }
    ()
  }

  it should "delete only what matches the criteria" in {
    new Fixture {
      withSnapshot { snapshot =>

        snapshot.insert(records: _*)
        snapshot.size should be(4)

        underTest.deleteMatchingSnapshots("unit-test", 30, 350)
        snapshot.size should be(1)

        snapshot.findOne($and(PROCESSOR_ID $eq "unit-test", SEQUENCE_NUMBER $eq 30, TIMESTAMP $eq 10000)) shouldBe defined

      }
    }
    ()
  }

  it should "delete only what matches the criteria in suffixed snapshot" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>

        snapshot.insert(records: _*)
        
        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)
        
        snapshot.size should be(4)

        underExtendedTest.deleteMatchingSnapshots("unit-test", 30, 350)
        snapshot.size should be(1)

        snapshot.findOne($and(PROCESSOR_ID $eq "unit-test", SEQUENCE_NUMBER $eq 30, TIMESTAMP $eq 10000)) shouldBe defined

      }
    }
    ()
  }

  it should "read legacy snapshot formats" in {
    new Fixture {
      withSnapshot { snapshot =>
        val legacies = records.map(CasbahPersistenceSnapshotter.legacySerializeSnapshot)
        snapshot.insert(legacies: _*)
        snapshot.size should be(4)

        snapshot.foreach { dbo =>
          deserializeSnapshot(dbo).metadata.persistenceId should be("unit-test")
        }
      }
    }
    ()
  }

  it should "read legacy suffixed snapshot formats" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>
        val legacies = records.map(CasbahPersistenceSnapshotter.legacySerializeSnapshot)
        snapshot.insert(legacies: _*)
        
        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)
        
        snapshot.size should be(4)

        snapshot.foreach { dbo =>
          deserializeSnapshot(dbo).metadata.persistenceId should be("unit-test")
        }
      }
    }
    ()
  }

  it should "read mixed snapshot formats" in {
    new Fixture {
      withSnapshot { snapshot =>
        val legacies = records.take(2).map(CasbahPersistenceSnapshotter.legacySerializeSnapshot)
        val newVersions = records.drop(2).map(CasbahPersistenceSnapshotter.serializeSnapshot)
        snapshot.insert(legacies ++ newVersions: _*)
        snapshot.size should be(4)

        snapshot.foreach { dbo =>
          deserializeSnapshot(dbo).metadata.persistenceId should be("unit-test")
        }
      }
    }
    ()
  }

  it should "read mixed suffixed snapshot formats" in {
    new Fixture {
      withSuffixedSnapshot(pid) { snapshot =>
        val legacies = records.take(2).map(CasbahPersistenceSnapshotter.legacySerializeSnapshot)
        val newVersions = records.drop(2).map(CasbahPersistenceSnapshotter.serializeSnapshot)
        snapshot.insert(legacies ++ newVersions: _*)
        
        val snapsName = extendedDriver.getSnapsCollectionName(pid)
        snapsName should be("akka_persistence_snaps_unit-test-test")
        extendedDriver.db.collectionExists(snapsName) should be(true)
        
        snapshot.size should be(4)

        snapshot.foreach { dbo =>
          deserializeSnapshot(dbo).metadata.persistenceId should be("unit-test")
        }
      }
    }
    ()
  }
}