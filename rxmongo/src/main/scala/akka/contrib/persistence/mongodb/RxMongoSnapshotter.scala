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
  import driver.RxMongoSerializers._

  private[this] val writeConcern = driver.snapsWriteConcern

  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = {
    val selected =
      snaps(pid).flatMap(
        _.find(BSONDocument(
          PROCESSOR_ID -> pid,
          SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
          TIMESTAMP -> BSONDocument("$lte" -> maxTs)
        ), Option.empty[BSONDocument])
          .sort(BSONDocument(SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
          .one[SelectedSnapshot]
      )
    selected
  }

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) = {
    val query = BSONDocument(
      PROCESSOR_ID -> snapshot.metadata.persistenceId,
      SEQUENCE_NUMBER -> snapshot.metadata.sequenceNr,
      TIMESTAMP -> snapshot.metadata.timestamp)
    snaps(snapshot.metadata.persistenceId)
      .flatMap(
        _.update(ordered = true, writeConcern)
          .one(query, snapshot, upsert = true, multi = false)
      ).map(_ => ())
  }

  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) = {
    val criteria =
      Seq[Producer[BSONElement]](PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq) ++
        Option[Producer[BSONElement]](TIMESTAMP -> ts).filter(_ => ts > 0).toSeq

    for {
      s <- snaps(pid)
      wr <- s.delete().one(BSONDocument(criteria: _*))
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && wr.ok)
        driver.removeEmptySnapshot(s)
          .map(_ => driver.removeSnapsInCache(pid))
      ()
    }
  }

  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = {
    for {
      s <- snaps(pid)
      wr <- s.delete()
              .one(BSONDocument(
                PROCESSOR_ID -> pid,
                SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
                TIMESTAMP -> BSONDocument("$lte" -> maxTs)
              ))
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty && wr.ok)
        driver.removeEmptySnapshot(s)
          .map(_ => driver.removeSnapsInCache(pid))
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
