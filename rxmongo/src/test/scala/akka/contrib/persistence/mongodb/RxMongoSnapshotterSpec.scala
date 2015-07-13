package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.RxMongoSerializers.RxMongoSnapshotSerialization
import akka.persistence.{SnapshotMetadata, SelectedSnapshot}
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import reactivemongo.bson.BSONDocument

import scala.concurrent._
import duration._

@RunWith(classOf[JUnitRunner])
class RxMongoSnapshotterSpec extends TestKit(ActorSystem("unit-test")) with RxMongoPersistenceSpec {

  implicit val serialization = SerializationExtension.get(system)
  implicit val serializer = new RxMongoSnapshotSerialization()

  "A rxmongo snapshotter" should "support legacy snapshots" in { withSnapshot { ss =>

    val metadata = (1 to 10).map(i => SnapshotMetadata("p-1",i,i))
    val snapshots = metadata.map(SelectedSnapshot(_,"snapshot"))
    val legacyDocs = snapshots.map(serializer.legacyWrite)

    Await.result(ss.bulkInsert(legacyDocs.toStream, ordered = true),3.seconds).n should be (metadata.size)

    val extracted = ss.find(BSONDocument()).cursor[SelectedSnapshot]().collect[List](stopOnError = true)
    val result = Await.result(extracted,3.seconds)
    result.size should be (10)
    result.head.metadata.persistenceId should be ("p-1")
  }}

  it should "support mixed snapshots" in { withSnapshot { ss =>

    val metadata = (1 to 10).map(i => SnapshotMetadata("p-1",i,i))
    val snapshots = metadata.map(SelectedSnapshot(_,"snapshot"))
    val legacyDocs = snapshots.take(5).map(serializer.legacyWrite)
    val newDocs = snapshots.drop(5).map(serializer.write)

    Await.result(ss.bulkInsert((legacyDocs ++ newDocs).toStream, ordered = true),3.seconds).n should be (metadata.size)

    val extracted = ss.find(BSONDocument()).cursor[SelectedSnapshot]().collect[List](stopOnError = true)
    val result = Await.result(extracted,3.seconds)
    result.size should be (10)
    result.foreach { sn =>
      sn.metadata.persistenceId should be ("p-1")
    }
  }}

}
