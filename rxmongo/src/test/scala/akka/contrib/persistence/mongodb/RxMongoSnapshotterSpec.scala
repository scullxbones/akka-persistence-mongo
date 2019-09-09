/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence.{SelectedSnapshot, SnapshotMetadata}
import akka.serialization.{Serialization, SerializationExtension}
import akka.testkit._
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.junit.JUnitRunner
import reactivemongo.api.Cursor
import reactivemongo.bson.BSONDocument

import scala.concurrent._
import duration._

@RunWith(classOf[JUnitRunner])
class RxMongoSnapshotterSpec extends TestKit(ActorSystem("unit-test")) with RxMongoPersistenceSpec with ScalaFutures with Eventually {

  override def embedDB = "persistence-snapshotter-rxmongo"

  implicit val serialization: Serialization = SerializationExtension.get(system)
  implicit val serializer = RxMongoSerializersExtension(system).RxMongoSnapshotSerialization
  implicit val as: ActorSystem = system

  val pid = "unit-test"

  "A rxmongo snapshotter" should "support legacy snapshots" in {
    withSnapshot { ss =>

      val metadata = (1L to 10L).map(i => SnapshotMetadata("p-1", i, i))
      val snapshots = metadata.map(SelectedSnapshot(_, "snapshot-1"))
      val legacyDocs = snapshots.map(serializer.legacyWrite)

      Await.result(ss.insert(ordered = true).many(legacyDocs), 3.seconds.dilated).n should be(metadata.size)

      val extracted = ss.find(BSONDocument.empty, Option.empty[BSONDocument]).cursor[SelectedSnapshot]().collect(Int.MaxValue, Cursor.FailOnError[List[SelectedSnapshot]]())
      val result = Await.result(extracted, 3.seconds.dilated)
      result.size should be(10)
      result.head.metadata.persistenceId should be("p-1")
      ()
    }
  }

  it should "support legacy snapshots in suffixed snapshot collection" in {
    withSuffixedSnapshot(pid) { ss =>

      val metadata = (1L to 10L).map(i => SnapshotMetadata("p-2", i, i))
      val snapshots = metadata.map(SelectedSnapshot(_, "snapshot-2"))
      val legacyDocs = snapshots.map(serializer.legacyWrite)

      Await.result(ss.insert(ordered = true).many(legacyDocs), 3.seconds.dilated).n should be(metadata.size)

      // should 'retrieve' (and not 'build') the suffixed snapshot 
      val snapsName = extendedDriver.getSnapsCollectionName(pid)
      snapsName should be("akka_persistence_snaps_unit-test-test")
      val collections = Await.result(extendedDriver.db.flatMap(_.collectionNames), 3.seconds.dilated)
      collections.contains(snapsName) should be (true)

      val extracted = ss.find(BSONDocument.empty, Option.empty[BSONDocument]).cursor[SelectedSnapshot]().collect(Int.MaxValue, Cursor.FailOnError[List[SelectedSnapshot]]())
      val result = Await.result(extracted, 3.seconds.dilated)
      result.size should be(10)
      result.head.metadata.persistenceId should be("p-2")
      ()
    }
  }

  it should "support mixed snapshots" in {
    withSnapshot { ss =>

      val metadata = (1L to 10L).map(i => SnapshotMetadata("p-3", i, i))
      val snapshots = metadata.map(SelectedSnapshot(_, "snapshot-3"))
      val legacyDocs = snapshots.take(5).map(serializer.legacyWrite)
      val newDocs = snapshots.drop(5).map(serializer.write)

      whenReady(ss.insert(ordered = true).many(legacyDocs ++ newDocs), PatienceConfiguration.Timeout(3.seconds.dilated)){
        _.n shouldBe metadata.size
      }

      val extracted = ss.find(BSONDocument.empty, Option.empty[BSONDocument]).cursor[SelectedSnapshot]().collect(Int.MaxValue, Cursor.FailOnError[List[SelectedSnapshot]]())
      whenReady(extracted, PatienceConfiguration.Timeout(3.seconds.dilated)){ result =>
        result should have size 10
        result.foreach { sn =>
          sn.metadata.persistenceId should be("p-3")
        }
        ()
      }
    }
  }

  it should "support mixed snapshots in suffixed snapshot collection" in {
    withSuffixedSnapshot(pid) { ss =>

      val metadata = (1L to 10L).map(i => SnapshotMetadata("p-4", i, i))
      val snapshots = metadata.map(SelectedSnapshot(_, "snapshot-4"))
      val legacyDocs = snapshots.take(5).map(serializer.legacyWrite)
      val newDocs = snapshots.drop(5).map(serializer.write)

      Await.result(ss.insert(ordered = true).many(legacyDocs ++ newDocs), 3.seconds.dilated).n should be(metadata.size)
       
      val snapsName = extendedDriver.getSnapsCollectionName(pid)
      snapsName should be("akka_persistence_snaps_unit-test-test")
      val collections = Await.result(extendedDriver.db.flatMap(_.collectionNames), 3.seconds.dilated)
      collections.contains(snapsName) should be (true)
      
      val extracted =
        ss.find(BSONDocument.empty, Option.empty[BSONDocument])
          .cursor[SelectedSnapshot]()
          .collect(Int.MaxValue, Cursor.FailOnError[List[SelectedSnapshot]]())

      eventually(PatienceConfiguration.Timeout(3.seconds.dilated)){
        val result = extracted.futureValue //, 3.seconds.dilated)
        result.size should be(10)
        result.foreach { sn =>
          sn.metadata.persistenceId should be("p-4")
        }
      }
    }
  }

  it should "save snapshot in suffixed snapshot collection" in {
    withAutoSuffixedSnapshot { drv =>
      
      val underExtendedTest = new RxMongoSnapshotter(drv)
      
      // should 'build' the suffixed snapshot
      underExtendedTest.saveSnapshot(SelectedSnapshot(SnapshotMetadata(pid, 4, 1000), "snapshot-payload"))
        .futureValue(PatienceConfiguration.Timeout(3.seconds.dilated))

      // should 'retrieve' (and not 'build') the suffixed snapshot 
      val snapsName = drv.getSnapsCollectionName(pid)
      snapsName should be("akka_persistence_snaps_unit-test-test")
      whenReady(drv.db.flatMap(_.collectionNames), PatienceConfiguration.Timeout(3.seconds.dilated))(_.contains(snapsName) shouldBe true)
      val ss = drv.getSnaps(pid)

      val extracted = ss.flatMap(
        _.find(BSONDocument.empty, Option.empty[SelectedSnapshot])
          .cursor[SelectedSnapshot]()
          .collect(Int.MaxValue, Cursor.FailOnError[List[SelectedSnapshot]]())
      )
      eventually(PatienceConfiguration.Timeout(3.seconds.dilated)){
        val result = extracted.futureValue
        result should have size 1
        result.head.metadata.persistenceId shouldBe pid
      }
    }
  }

}
