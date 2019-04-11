package akka.contrib.persistence.mongodb
import akka.persistence.serialization.Snapshot
import akka.persistence.{SelectedSnapshot, SnapshotMetadata}
import akka.serialization.Serialization
import org.mongodb.scala._
import org.mongodb.scala.bson.{BsonBinary, BsonDocument, BsonValue}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Indexes._
import org.mongodb.scala.model.{IndexOptions, ReplaceOptions}

import scala.concurrent.{ExecutionContext, Future}

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
      val sn = document.getLong(SEQUENCE_NUMBER)
      val ts = document.getLong(TIMESTAMP)
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

  override private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = {
    snaps(pid).flatMap(_.find(
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

  override private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) = {
    val query = and(
      equal(PROCESSOR_ID, snapshot.metadata.persistenceId),
      equal(SEQUENCE_NUMBER, snapshot.metadata.sequenceNr),
      equal(TIMESTAMP, snapshot.metadata.timestamp)
    )
    snaps(snapshot.metadata.persistenceId)
      .flatMap(
        _.replaceOne(
          query,
          serializeSnapshot(snapshot),
          new ReplaceOptions().upsert(true)
        ).toFuture()
      ).map(_ => ())
  }

  override private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) = {
    val criteria =
      Option(ts).filter(_ > 0).foldLeft(
        and(equal(PROCESSOR_ID,pid), equal(SEQUENCE_NUMBER,seq))
      ){ case(bson,stamp) => and(bson, equal(TIMESTAMP, stamp)) }

    for {
      s <- snaps(pid)
      wr <- s.deleteMany(criteria).toFuture()
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && wr.wasAcknowledged())
        for {
          n <- s.countDocuments().toFuture() if n == 0
          _ <- s.drop().toFuture()
        } yield ()
      ()
    }
  }

  override private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = {
    for {
      s <- snaps(pid)
      wr <- s.deleteMany(
              and(
                equal(PROCESSOR_ID, pid),
                lte(SEQUENCE_NUMBER, maxSeq),
                lte(TIMESTAMP, maxTs)
              )
            ).toFuture()
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && wr.wasAcknowledged())
        for {
          n <- s.countDocuments().toFuture() if n == 0L
          _ <- s.drop().toFuture()
        } yield ()
      ()
    }
  }

  private[this] def snaps(suffix: String)(implicit ec: ExecutionContext) = {
    driver.getSnaps(suffix).flatMap{ c =>
      c.createIndex(
        compoundIndex(
          ascending(SnapshottingFieldNames.PROCESSOR_ID),
          descending(SnapshottingFieldNames.SEQUENCE_NUMBER),
          descending(SnapshottingFieldNames.TIMESTAMP)
        ),
        new IndexOptions().background(true).unique(true).name(driver.snapsIndexName)
      ).toFuture().map(_ => c.withWriteConcern(driver.snapsWriteConcern))
    }
  }
}
