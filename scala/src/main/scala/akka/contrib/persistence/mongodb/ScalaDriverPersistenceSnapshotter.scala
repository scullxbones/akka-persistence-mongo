package akka.contrib.persistence.mongodb
import akka.persistence.serialization.Snapshot
import akka.persistence.{SelectedSnapshot, SnapshotMetadata}
import akka.serialization.Serialization
import org.mongodb.scala._
import org.mongodb.scala.bson.{BsonBinary, BsonDocument, BsonValue}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Indexes._
import org.mongodb.scala.model.ReplaceOptions

import scala.concurrent.Future

object ScalaDriverPersistenceSnapshotter extends SnapshottingFieldNames {

  def serializeSnapshot(snapshot: SelectedSnapshot)(implicit serialization: Serialization): BsonDocument = {
    val obj = BsonDocument(
      PROCESSOR_ID -> snapshot.metadata.persistenceId,
      SEQUENCE_NUMBER -> snapshot.metadata.sequenceNr,
      TIMESTAMP -> snapshot.metadata.timestamp
    )
    snapshot.snapshot match {
      case o: BsonValue =>
        obj.append(V2.SERIALIZED, o)
      case _ =>
        Serialization.withTransportInformation(serialization.system) { () =>
          obj.append(V2.SERIALIZED, BsonBinary(serialization.serializerFor(classOf[Snapshot]).toBinary(Snapshot(snapshot.snapshot))))
        }
    }
  }

  def deserializeSnapshot(document: BsonDocument)(implicit serialization: Serialization): SelectedSnapshot = {
    if (document.contains(V1.SERIALIZED)) {
      (for {
        content <- Option(document.get(V1.SERIALIZED)).filter(_.isBinary).map(_.asBinary).map(_.getData)
        ss      <- serialization.deserialize(content, classOf[SelectedSnapshot]).toOption
      } yield ss).get
    } else {
      val content = Option(document.get(V2.SERIALIZED)) match {
        case Some(b: BsonBinary) =>
          (for {
            content <- Option(b).filter(_.isBinary).map(_.asBinary).map(_.getData)
            snap    <- serialization.deserialize(content, classOf[Snapshot]).toOption
          } yield snap.data).get
        case Some(b: BsonValue) => b
        case _ => null
      }

      val pid = document.getString(PROCESSOR_ID).getValue
      val sn = document.getInt64(SEQUENCE_NUMBER).longValue()
      val ts = document.getInt64(TIMESTAMP).longValue()
      SelectedSnapshot(SnapshotMetadata(pid, sn, ts), content)
    }
  }

  @deprecated("Use v2 write instead", "0.3.0")
  def legacySerializeSnapshot(snapshot: SelectedSnapshot)(implicit serialization: Serialization): BsonDocument =
    BsonDocument(PROCESSOR_ID -> snapshot.metadata.persistenceId,
      SEQUENCE_NUMBER -> snapshot.metadata.sequenceNr,
      TIMESTAMP -> snapshot.metadata.timestamp,
      V1.SERIALIZED -> serialization.serializerFor(classOf[SelectedSnapshot]).toBinary(snapshot))
}

class ScalaDriverPersistenceSnapshotter(driver: ScalaMongoDriver) extends MongoPersistenceSnapshottingApi {
  import ScalaDriverPersistenceSnapshotter._
  import driver.ScalaSerializers.serialization
  import driver.pluginDispatcher

  override def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long): Future[Option[SelectedSnapshot]] = {
    val snaps = driver.getSnaps(pid)
    snaps.flatMap(_.find(
        and(
          equal(SnapshottingFieldNames.PROCESSOR_ID, pid),
          lte(SnapshottingFieldNames.SEQUENCE_NUMBER, maxSeq),
          lte(SnapshottingFieldNames.TIMESTAMP,maxTs)
        )
      )
      .sort(descending(SnapshottingFieldNames.SEQUENCE_NUMBER, SnapshottingFieldNames.TIMESTAMP))
      .first()
      .toFutureOption()
      .map(_.map(_.asDocument()).map(deserializeSnapshot))
      .recoverWith{
        case t: Throwable =>
          t.printStackTrace()
          Future.failed(t)
      }
    )
  }

  override def saveSnapshot(snapshot: SelectedSnapshot): Future[Unit] = {
    val snaps = driver.snaps(snapshot.metadata.persistenceId)
    val query = and(
      equal(PROCESSOR_ID, snapshot.metadata.persistenceId),
      equal(SEQUENCE_NUMBER, snapshot.metadata.sequenceNr),
      equal(TIMESTAMP, snapshot.metadata.timestamp)
    )

    snaps.map(_.withWriteConcern(driver.snapsWriteConcern))
      .flatMap(
        _.replaceOne(
          query,
          serializeSnapshot(snapshot),
          new ReplaceOptions().upsert(true)
        ).toFuture()
      ).map(_ => ())
  }

  override def deleteSnapshot(pid: String, seq: Long, ts: Long): Future[Unit] = {
    val snaps = driver.getSnaps(pid)
    val criteria =
      Option(ts).filter(_ > 0).foldLeft(
        and(equal(PROCESSOR_ID,pid), equal(SEQUENCE_NUMBER,seq))
      ){ case(bson,stamp) => and(bson, equal(TIMESTAMP, stamp)) }

    for {
      s0 <- snaps
      s = s0.withWriteConcern(driver.snapsWriteConcern)
      wr <- s.deleteMany(criteria).toFuture()
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && wr.wasAcknowledged())
        driver.removeEmptySnapshot(s)
            .map(_ => driver.removeSnapsInCache(pid))
      ()
    }
  }

  override def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long): Future[Unit] = {
    val snaps = driver.getSnaps(pid)
    for {
      s0 <- snaps
      s = s0.withWriteConcern(driver.snapsWriteConcern)
      wr <- s.deleteMany(
              and(
                equal(PROCESSOR_ID, pid),
                lte(SEQUENCE_NUMBER, maxSeq),
                lte(TIMESTAMP, maxTs)
              )
            ).toFuture()
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && wr.wasAcknowledged())
        driver.removeEmptySnapshot(s)
          .map(_ => driver.removeSnapsInCache(pid))
      ()
    }
  }

}
