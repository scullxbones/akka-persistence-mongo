package akka.contrib.persistence.mongodb

import akka.contrib.persistence.mongodb.SnapshottingFieldNames._
import akka.persistence.serialization.Snapshot
import akka.persistence.{SelectedSnapshot, SnapshotMetadata}
import akka.serialization.Serialization
import reactivemongo.api.indexes._
import reactivemongo.bson._

import scala.concurrent._

class RxMongoSnapshotSerialization(implicit serialization: Serialization) extends BSONDocumentReader[SelectedSnapshot] with BSONDocumentWriter[SelectedSnapshot] {

  import RxMongoPersistenceExtension._

  override def read(doc: BSONDocument): SelectedSnapshot = {
    val content = doc.getAs[Array[Byte]](V1.SERIALIZED)
    if (content.isDefined) {
      serialization.deserialize(content.get, classOf[SelectedSnapshot]).get
    } else {
      val pid = doc.getAs[String](PROCESSOR_ID).get
      val sn = doc.getAs[Long](SEQUENCE_NUMBER).get
      val timestamp = doc.getAs[Long](TIMESTAMP).get

      val content = doc.get(V2.SERIALIZED).get match {
        case b: BSONDocument =>
          b
        case _ =>
          val snapshot = doc.getAs[Array[Byte]](V2.SERIALIZED).get
          val deserialized = serialization.deserialize(snapshot, classOf[Snapshot]).get
          deserialized.data
      }

      SelectedSnapshot(SnapshotMetadata(pid,sn,timestamp),content)
    }
  }

  override def write(snap: SelectedSnapshot): BSONDocument = {
    val content: BSONValue = snap.snapshot match {
      case b: BSONDocument =>
        b
      case _ =>
        BsonBinaryHandler.write(serialization.serialize(Snapshot(snap.snapshot)).get)
    }
    BSONDocument(PROCESSOR_ID -> snap.metadata.persistenceId,
      SEQUENCE_NUMBER -> snap.metadata.sequenceNr,
      TIMESTAMP -> snap.metadata.timestamp,
      V2.SERIALIZED -> content)
  }

  @deprecated("Use v2 write instead", "0.3.0")
  def legacyWrite(snap: SelectedSnapshot): BSONDocument = {
    val content = serialization.serialize(snap).get
    BSONDocument(PROCESSOR_ID -> snap.metadata.persistenceId,
      SEQUENCE_NUMBER -> snap.metadata.sequenceNr,
      TIMESTAMP -> snap.metadata.timestamp,
      V1.SERIALIZED -> content)
  }
}


class RxMongoSnapshotter(driver: RxMongoPersistenceDriver) extends MongoPersistenceSnapshottingApi {

  import SnapshottingFieldNames._

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.snapsWriteConcern
  private[this] implicit lazy val snapshotSerialization = new RxMongoSnapshotSerialization()

  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = {
    val selected =
      snaps.find(
        BSONDocument(PROCESSOR_ID -> pid,
          SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
          TIMESTAMP -> BSONDocument("$lte" -> maxTs)
        )
      ).sort(BSONDocument(SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
        .one[SelectedSnapshot]
    selected
  }

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) =
    snaps.insert(snapshot, writeConcern).map(_ => ())

  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) =
    snaps.remove(BSONDocument(PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq, TIMESTAMP -> ts), writeConcern)

  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) =
    snaps.remove(BSONDocument(PROCESSOR_ID -> pid,
      SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
      TIMESTAMP -> BSONDocument("$lte" -> maxTs)),
      writeConcern)

  private[this] def snaps(implicit ec: ExecutionContext) = {
    val snaps = driver.collection(driver.snapsCollectionName)
    snaps.indexesManager.ensure(new Index(
      key = Seq((PROCESSOR_ID, IndexType.Ascending),
        (SEQUENCE_NUMBER, IndexType.Descending),
        (TIMESTAMP, IndexType.Descending)),
      background = true,
      unique = true,
      name = Some(driver.snapsIndexName)))
    snaps
  }

}
