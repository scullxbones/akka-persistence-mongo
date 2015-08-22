package akka.contrib.persistence.mongodb

import akka.actor.Actor
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
   * Plugin API: deletes the snapshot identified by `metadata`.
   *
   * @param metadata snapshot metadata.
   */

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    impl.deleteSnapshot(metadata.persistenceId, metadata.sequenceNr, metadata.timestamp)

  /**
   * Plugin API: deletes all snapshots matching `criteria`.
   *
   * @param persistenceId id of the persistent actor.
   * @param criteria selection criteria for deleting.
   */
  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    impl.deleteMatchingSnapshots(persistenceId, criteria.maxSequenceNr, criteria.maxTimestamp)

  /**
   * Plugin API
   * Allows plugin implementers to use `f pipeTo self` and
   * handle additional messages for implementing advanced features
   */
  override def receivePluginInternal: Actor.Receive = Actor.emptyBehavior
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
  
  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext): Future[Unit]
  
  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext): Future[Unit]
}

trait MongoPersistenceSnapshotFailFast extends MongoPersistenceSnapshottingApi {

  private[mongodb] val breaker: CircuitBreaker

  private[mongodb] abstract override def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) =
    breaker.withCircuitBreaker(super.findYoungestSnapshotByMaxSequence(pid,maxSeq,maxTs))

  private[mongodb] abstract override def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) =
    breaker.withCircuitBreaker(super.saveSnapshot(snapshot))

  private[mongodb] abstract override def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) =
    breaker.withCircuitBreaker(super.deleteSnapshot(pid,seq,ts))

  private[mongodb] abstract override def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) =
    breaker.withCircuitBreaker(super.deleteMatchingSnapshots(pid,maxSeq,maxTs))
}