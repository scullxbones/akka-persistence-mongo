package com.github.scullxbones.mongka

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._

import akka.persistence._
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.serialization.Snapshot
import akka.serialization._

import reactivemongo.bson._

class MongkaSnapshots extends SnapshotStore {

  import serializers._

  private[this] val serialization = SerializationExtension(context.system)
  private[this] val extension = context.system.extension(MongkaExtensionId)
  private[this] val snaps = extension.snaps
  private[this] val breaker = extension.breaker

  implicit object SelectedSnapshotHandler extends BSONDocumentReader[SelectedSnapshot] with BSONDocumentWriter[SelectedSnapshot] {
    def read(doc: BSONDocument): SelectedSnapshot = {
      val content = doc.getAs[Array[Byte]]("sn").get
      val snapshot = serialization.deserialize(content, classOf[Snapshot])
      SelectedSnapshot(
        SnapshotMetadata(doc.getAs[String]("pid").get, doc.getAs[Long]("sq").get, doc.getAs[Long]("ts").get),
        snapshot)
    }

    def write(snap: SelectedSnapshot): BSONDocument = {
      val content = serialization.serialize(snap.snapshot.asInstanceOf[AnyRef]).get
      BSONDocument("pid" -> snap.metadata.processorId,
        "sq" -> snap.metadata.sequenceNr,
        "ts" -> snap.metadata.timestamp,
        "sn" -> content)
    }
  }

  /**
   * Plugin API.
   *
   * Asynchronously loads a snapshot.
   *
   * @param pid processor id.
   * @param criteria selection criteria for loading.
   */
  def loadAsync(pid: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    breaker.withCircuitBreaker {
      val selected =
        snaps.find(
          BSONDocument("pid" -> pid,
            "sq" -> BSONDocument("$lte" -> criteria.maxSequenceNr),
            "ts" -> BSONDocument("$lte" -> criteria.maxTimestamp))).sort(BSONDocument("sq" -> -1, "ts" -> -1))
          .one[SelectedSnapshot]
      selected
    }

  /**
   * Plugin API.
   *
   * Asynchronously saves a snapshot.
   *
   * @param metadata snapshot metadata.
   * @param snapshot snapshot.
   */
  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    breaker.withCircuitBreaker(snaps.insert(SelectedSnapshot(metadata, snapshot)).mapTo[Unit])

  /**
   * Plugin API.
   *
   * Called after successful saving of a snapshot.
   *
   * @param metadata snapshot metadata.
   */
  def saved(metadata: SnapshotMetadata) = ()

  /**
   * Plugin API.
   *
   * Deletes the snapshot identified by `metadata`.
   *
   * @param metadata snapshot metadata.
   */
  def delete(metadata: SnapshotMetadata) =
    breaker.withCircuitBreaker(snaps.remove(BSONDocument("pid" -> metadata.processorId,
      "sq" -> metadata.sequenceNr,
      "ts" -> metadata.timestamp)))

}