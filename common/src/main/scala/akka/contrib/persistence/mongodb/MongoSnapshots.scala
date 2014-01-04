package akka.contrib.persistence.mongodb

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.SnapshotSelectionCriteria
import scala.concurrent.Future
import akka.persistence.SnapshotMetadata
import akka.persistence.SelectedSnapshot

class MongoSnapshots extends SnapshotStore {

  private[this] val impl = context.system.extension(MongoPersistenceExtensionId).snapshotter
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