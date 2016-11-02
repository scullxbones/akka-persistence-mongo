/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.RxMongoSerializers.RxMongoSnapshotSerialization
import akka.persistence.{ SnapshotMetadata, SelectedSnapshot }
import akka.serialization.SerializationExtension
import akka.testkit._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import reactivemongo.bson.BSONDocument

import scala.concurrent._
import duration._

@RunWith(classOf[JUnitRunner])
class RxMongoSnapshotterSpec extends TestKit(ActorSystem("unit-test")) with RxMongoPersistenceSpec {

  override def embedDB = "persistence-snapshotter-rxmongo"

  implicit val serialization = SerializationExtension.get(system)
  implicit val serializer = new RxMongoSnapshotSerialization()
  implicit val as = system

  val pid = "unit-test"

  "A rxmongo snapshotter" should "support legacy snapshots" in {
    withSnapshot { ss =>

      val metadata = (1L to 10L).map(i => SnapshotMetadata("p-1", i, i))
      val snapshots = metadata.map(SelectedSnapshot(_, "snapshot"))
      val legacyDocs = snapshots.map(serializer.legacyWrite)

      Await.result(ss.bulkInsert(legacyDocs.toStream, ordered = true), 3.seconds.dilated).n should be(metadata.size)

      val extracted = ss.find(BSONDocument()).cursor[SelectedSnapshot]().collect[List](stopOnError = true)
      val result = Await.result(extracted, 3.seconds.dilated)
      result.size should be(10)
      result.head.metadata.persistenceId should be("p-1")
      ()
    }
  }

  it should "support legacy snapshots in suffixed snapshot collection" in {
    withSuffixedSnapshot(pid) { ss =>

      val metadata = (1L to 10L).map(i => SnapshotMetadata("p-1", i, i))
      val snapshots = metadata.map(SelectedSnapshot(_, "snapshot"))
      val legacyDocs = snapshots.map(serializer.legacyWrite)

      Await.result(ss.bulkInsert(legacyDocs.toStream, ordered = true), 3.seconds.dilated).n should be(metadata.size)

      // should 'retrieve' (and not 'build') the suffixed snapshot 
      val snapsName = extendedDriver.getSnapsCollectionName(pid)
      snapsName should be("akka_persistence_snaps_unit-test-test")
      val collections = Await.result(extendedDriver.db.flatMap(_.collectionNames), 3.seconds.dilated)
      collections.contains(snapsName) should be (true)

      val extracted = ss.find(BSONDocument()).cursor[SelectedSnapshot]().collect[List](stopOnError = true)
      val result = Await.result(extracted, 3.seconds.dilated)
      result.size should be(10)
      result.head.metadata.persistenceId should be("p-1")
      ()
    }
  }

  it should "support mixed snapshots" in {
    withSnapshot { ss =>

      val metadata = (1L to 10L).map(i => SnapshotMetadata("p-1", i, i))
      val snapshots = metadata.map(SelectedSnapshot(_, "snapshot"))
      val legacyDocs = snapshots.take(5).map(serializer.legacyWrite)
      val newDocs = snapshots.drop(5).map(serializer.write)

      Await.result(ss.bulkInsert((legacyDocs ++ newDocs).toStream, ordered = true), 3.seconds.dilated).n should be(metadata.size)

      val extracted = ss.find(BSONDocument()).cursor[SelectedSnapshot]().collect[List](stopOnError = true)
      val result = Await.result(extracted, 3.seconds.dilated.dilated)
      result.size should be(10)
      result.foreach { sn =>
        sn.metadata.persistenceId should be("p-1")
      }
      ()
    }
  }

  it should "support mixed snapshots in suffixed snapshot collection" in {
    withSuffixedSnapshot(pid) { ss =>

      val metadata = (1L to 10L).map(i => SnapshotMetadata("p-1", i, i))
      val snapshots = metadata.map(SelectedSnapshot(_, "snapshot"))
      val legacyDocs = snapshots.take(5).map(serializer.legacyWrite)
      val newDocs = snapshots.drop(5).map(serializer.write)

      Await.result(ss.bulkInsert((legacyDocs ++ newDocs).toStream, ordered = true), 3.seconds.dilated).n should be(metadata.size)
       
      val snapsName = extendedDriver.getSnapsCollectionName(pid)
      snapsName should be("akka_persistence_snaps_unit-test-test")
      val collections = Await.result(extendedDriver.db.flatMap(_.collectionNames), 3.seconds.dilated)
      collections.contains(snapsName) should be (true)
      
      val extracted = ss.find(BSONDocument()).cursor[SelectedSnapshot]().collect[List](stopOnError = true)
      val result = Await.result(extracted, 3.seconds.dilated)
      result.size should be(10)
      result.foreach { sn =>
        sn.metadata.persistenceId should be("p-1")
      }
      ()
    }
  }

  it should "save snapshot in suffixed snapshot collection" in {
    withAutoSuffixedSnapshot { drv =>
      
      val underExtendedTest = new RxMongoSnapshotter(drv)
      
      // should 'build' the suffixed snapshot
      Await.ready(new RxMongoSnapshotter(drv).saveSnapshot(SelectedSnapshot(SnapshotMetadata(pid, 4, 1000), "snapshot-payload")), 3.seconds)

      // should 'retrieve' (and not 'build') the suffixed snapshot 
      val snapsName = drv.getSnapsCollectionName(pid)
      snapsName should be("akka_persistence_snaps_unit-test-test")
      val collections = Await.result(drv.db.flatMap(_.collectionNames), 3.seconds.dilated)
      collections.contains(snapsName) should be (true)
      val ss = drv.getSnaps(pid)

      val extracted = ss.flatMap(_.find(BSONDocument()).cursor[SelectedSnapshot]().collect[List](stopOnError = true))
      val result = Await.result(extracted, 3.seconds.dilated)
      result.size should be(1)
      result.head.metadata.persistenceId should be(pid)
      ()
    }
  }

}
