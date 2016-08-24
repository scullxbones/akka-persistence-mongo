/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.persistence.SelectedSnapshot
import reactivemongo.api.indexes._
import reactivemongo.bson._

import scala.concurrent._

class RxMongoSnapshotter(driver: RxMongoDriver) extends MongoPersistenceSnapshottingApi {

  import SnapshottingFieldNames._
  import RxMongoSerializers._

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.snapsWriteConcern
  private[this] implicit lazy val snapshotSerialization = new RxMongoSnapshotSerialization()

  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = {
    val selected =
      snaps(pid).find(
        BSONDocument(PROCESSOR_ID -> pid,
          SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
          TIMESTAMP -> BSONDocument("$lte" -> maxTs)
        )
      ).sort(BSONDocument(SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
        .one[SelectedSnapshot]
    selected
  }

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) = {
    val query = BSONDocument(
      PROCESSOR_ID -> snapshot.metadata.persistenceId,
      SEQUENCE_NUMBER -> snapshot.metadata.sequenceNr,
      TIMESTAMP -> snapshot.metadata.timestamp
    )
    snaps(snapshot.metadata.persistenceId).update(query, snapshot, writeConcern, upsert = true, multi = false).map(_ => ())
  }

  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) = {
    val criteria =
      Seq[Producer[BSONElement]](PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq) ++
        Option[Producer[BSONElement]](TIMESTAMP -> ts).filter(_ => ts > 0).toSeq
    snaps(pid).remove(BSONDocument(criteria : _*), writeConcern).map(_ => ())
  }

  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) =
    snaps(pid).remove(BSONDocument(PROCESSOR_ID -> pid,
                              SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
                              TIMESTAMP -> BSONDocument("$lte" -> maxTs)),
                              writeConcern).map(_ => ())

  private[this] def snaps(suffix: String)(implicit ec: ExecutionContext) = {
    val snaps = driver.getSnaps(suffix)
    snaps.indexesManager.ensure(new Index(
      key = Seq((PROCESSOR_ID, IndexType.Ascending),
        (SEQUENCE_NUMBER, IndexType.Descending),
        (TIMESTAMP, IndexType.Descending)),
      background = true,
      unique = true,
      name = Some(driver.getSnapsIndexName(suffix))))
    snaps
  }

}
