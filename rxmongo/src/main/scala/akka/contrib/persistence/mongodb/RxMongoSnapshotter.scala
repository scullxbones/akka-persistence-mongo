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
      snaps(pid).flatMap(_.find(
        BSONDocument(PROCESSOR_ID -> pid,
          SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
          TIMESTAMP -> BSONDocument("$lte" -> maxTs))).sort(BSONDocument(SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
        .one[SelectedSnapshot])
    selected
  }

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) = {
    val query = BSONDocument(
      PROCESSOR_ID -> snapshot.metadata.persistenceId,
      SEQUENCE_NUMBER -> snapshot.metadata.sequenceNr,
      TIMESTAMP -> snapshot.metadata.timestamp)
    snaps(snapshot.metadata.persistenceId).flatMap(_.update(query, snapshot, writeConcern, upsert = true, multi = false)).map(_ => ())
  }

  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) = {
    val criteria =
      Seq[Producer[BSONElement]](PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq) ++
        Option[Producer[BSONElement]](TIMESTAMP -> ts).filter(_ => ts > 0).toSeq

    for {
      s <- snaps(pid)
      wr <- s.remove(BSONDocument(criteria: _*), writeConcern)
    } yield {
      if (driver.useSuffixedCollectionNames && wr.ok)
        for {
          n <- s.count()
          if (n == 0)
          _ <- s.drop(failIfNotFound = false)
        } yield ()
      ()
    }
  }

  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = {
    for {
      s <- snaps(pid)
      wr <- s.remove(BSONDocument(PROCESSOR_ID -> pid,
        SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
        TIMESTAMP -> BSONDocument("$lte" -> maxTs)),
        writeConcern)
    } yield {
      if (driver.useSuffixedCollectionNames && wr.ok)
        for {
          n <- s.count()
          if (n == 0)
          _ <- s.drop(failIfNotFound = false)
        } yield ()
      ()
    }
  }

  private[this] def snaps(suffix: String)(implicit ec: ExecutionContext) = {
    val snaps = driver.getSnaps(suffix)
    snaps.flatMap(_.indexesManager.ensure(Index(
      key = Seq((PROCESSOR_ID, IndexType.Ascending),
        (SEQUENCE_NUMBER, IndexType.Descending),
        (TIMESTAMP, IndexType.Descending)),
      background = true,
      unique = true,
      name = Some(driver.snapsIndexName))))
    snaps
  }

}
