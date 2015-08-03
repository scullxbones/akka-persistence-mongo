package akka.contrib.persistence.mongodb

import akka.persistence.serialization.Snapshot
import akka.persistence.{SelectedSnapshot, SnapshotMetadata}
import akka.serialization.Serialization
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._

import scala.concurrent._
import scala.language.implicitConversions

object CasbahPersistenceSnapshotter {

  import SnapshottingFieldNames._

  implicit def serializeSnapshot(snapshot: SelectedSnapshot)(implicit serialization: Serialization): DBObject = {
    val obj = MongoDBObject(PROCESSOR_ID -> snapshot.metadata.persistenceId,
      SEQUENCE_NUMBER -> snapshot.metadata.sequenceNr,
      TIMESTAMP -> snapshot.metadata.timestamp)
    snapshot.snapshot match {
      case o: DBObject =>
        obj.put(V2.SERIALIZED, o)
      case b =>
        obj.put(V2.SERIALIZED, serialization.serializerFor(classOf[Snapshot]).toBinary(Snapshot(snapshot.snapshot)))
    }
    obj
  }

  implicit def deserializeSnapshot(document: DBObject)(implicit serialization: Serialization): SelectedSnapshot = {
    if (document.containsField(V1.SERIALIZED)) {
      val content = document.as[Array[Byte]](V1.SERIALIZED)
      serialization.deserialize(content, classOf[SelectedSnapshot]).get
    } else {


      val content = document.get(V2.SERIALIZED) match {
        case o: DBObject =>
          o
        case _ =>
          val content = document.as[Array[Byte]](V2.SERIALIZED)
          val snap = serialization.deserialize(content, classOf[Snapshot]).get
          snap.data
      }

      val pid = document.as[String](PROCESSOR_ID)
      val sn = document.as[Long](SEQUENCE_NUMBER)
      val ts = document.as[Long](TIMESTAMP)
      SelectedSnapshot(SnapshotMetadata(pid, sn, ts), content)
    }
  }

  @deprecated("Use v2 write instead", "0.3.0")
  def legacySerializeSnapshot(snapshot: SelectedSnapshot)(implicit serialization: Serialization): DBObject =
    MongoDBObject(PROCESSOR_ID -> snapshot.metadata.persistenceId,
      SEQUENCE_NUMBER -> snapshot.metadata.sequenceNr,
      TIMESTAMP -> snapshot.metadata.timestamp,
      V1.SERIALIZED -> serialization.serializerFor(classOf[SelectedSnapshot]).toBinary(snapshot))
}

class CasbahPersistenceSnapshotter(driver: CasbahMongoDriver) extends MongoPersistenceSnapshottingApi {

  import CasbahPersistenceSnapshotter._
  import SnapshottingFieldNames._

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.snapsWriteConcern

  private[this] def snaps(implicit ec: ExecutionContext) = driver.snaps

  private[this] def snapQueryMaxSequenceMaxTime(pid: String, maxSeq: Long, maxTs: Long) =
    $and(PROCESSOR_ID $eq pid, SEQUENCE_NUMBER $lte maxSeq, TIMESTAMP $lte maxTs)

  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = Future {
    snaps.find(snapQueryMaxSequenceMaxTime(pid, maxSeq, maxTs))
      .sort(MongoDBObject(SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
      .limit(1)
      .collectFirst {
      case o: DBObject => deserializeSnapshot(o)
    }
  }

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) = Future {
    snaps.insert(snapshot, writeConcern)
  }

  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) = Future {
    snaps.remove($and(PROCESSOR_ID $eq pid, SEQUENCE_NUMBER $eq seq, TIMESTAMP $eq ts), writeConcern)
  }

  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = Future {
    snaps.remove(snapQueryMaxSequenceMaxTime(pid, maxSeq, maxTs), writeConcern)
  }
}