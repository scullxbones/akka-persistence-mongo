package akka.contrib.persistence.mongodb

import akka.pattern.CircuitBreaker
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.SnapshotSelectionCriteria
import scala.concurrent.Future
import akka.persistence.SnapshotMetadata
import akka.persistence.SelectedSnapshot
import scala.concurrent.ExecutionContext

class MongoSnapshots extends SnapshotStore {

  private[this] val impl = MongoPersistenceExtension(context.system).snapshotter
  private[this] implicit val ec = context.dispatcher
  
  /**
   * Plugin API: asynchronously loads a snapshot.
   *
   * @param processorId processor id.
   * @param criteria selection criteria for loading.
   */
  override def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria) = 
    impl.findYoungestSnapshotByMaxSequence(processorId, criteria.maxSequenceNr, criteria.maxTimestamp)

  /**
   * Plugin API: asynchronously saves a snapshot.
   *
   * @param metadata snapshot metadata.
   * @param snapshot snapshot.
   */
  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any) = 
    impl.saveSnapshot(SelectedSnapshot(metadata,snapshot))

  /**
   * Plugin API: called after successful saving of a snapshot.
   *
   * @param metadata snapshot metadata.
   */
  override def saved(metadata: SnapshotMetadata) = ()

  /**
   * Plugin API: deletes the snapshot identified by `metadata`.
   *
   * @param metadata snapshot metadata.
   */

  override def delete(metadata: SnapshotMetadata) = 
    impl.deleteSnapshot(metadata.processorId,metadata.sequenceNr,metadata.timestamp)

  /**
   * Plugin API: deletes all snapshots matching `criteria`.
   *
   * @param processorId processor id.
   * @param criteria selection criteria for deleting.
   */
  override def delete(processorId: String, criteria: SnapshotSelectionCriteria) = 
    impl.deleteMatchingSnapshots(processorId, criteria.maxSequenceNr, criteria.maxTimestamp)
}

object SnapshottingFieldNames {
  final val PROCESSOR_ID = "pid"
  final val SEQUENCE_NUMBER = "sn"
  final val TIMESTAMP = "ts"
  object V1 {
    final val SERIALIZED = "ss"
  }
  object V2 {
    final val SERIALIZED = "s2"
  }
}

trait MongoPersistenceSnapshottingApi {
  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext): Future[Option[SelectedSnapshot]]

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext): Future[Unit]
  
  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext): Unit
  
  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext): Unit
}

trait MongoPersistenceSnapshotFailFast extends MongoPersistenceSnapshottingApi {

  private[mongodb] val breaker: CircuitBreaker

  private[mongodb] abstract override def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) =
    breaker.withCircuitBreaker(super.findYoungestSnapshotByMaxSequence(pid,maxSeq,maxTs))

  private[mongodb] abstract override def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) =
    breaker.withCircuitBreaker(super.saveSnapshot(snapshot))

  private[mongodb] abstract override def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) =
    breaker.withSyncCircuitBreaker(super.deleteSnapshot(pid,seq,ts))

  private[mongodb] abstract override def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) =
    breaker.withSyncCircuitBreaker(super.deleteMatchingSnapshots(pid,maxSeq,maxTs))
}