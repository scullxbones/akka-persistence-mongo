/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.persistence.SelectedSnapshot
import reactivemongo.api.indexes._
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection.BSONSerializationPack

import scala.concurrent._

class RxMongoSnapshotter(driver: RxMongoDriver) extends MongoPersistenceSnapshottingApi {

  import SnapshottingFieldNames._
  import driver.RxMongoSerializers._
  import driver.pluginDispatcher

  private[this] val writeConcern = driver.snapsWriteConcern

  override def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long): Future[Option[SelectedSnapshot]] = {
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

  override def saveSnapshot(snapshot: SelectedSnapshot): Future[Unit] = {
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

  override def deleteSnapshot(pid: String, seq: Long, ts: Long): Future[Unit] = {
    val criteria =
      Seq[ElementProducer](PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq) ++
        Option[ElementProducer](TIMESTAMP -> ts).filter(_ => ts > 0).toSeq

    for {
      s <- snaps(pid)
      wr <- s.delete().one(BSONDocument(criteria: _*))
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty)
        driver.removeEmptySnapshot(s)
          .map(_ => driver.removeSnapsInCache(pid))
      ()
    }
  }

  override def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long): Future[Unit] = {
    for {
      s <- snaps(pid)
      wr <- s.delete()
              .one(BSONDocument(
                PROCESSOR_ID -> pid,
                SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
                TIMESTAMP -> BSONDocument("$lte" -> maxTs)
              ))
    } yield {
      if (driver.useSuffixedCollectionNames && driver.suffixDropEmpty)
        driver.removeEmptySnapshot(s)
          .map(_ => driver.removeSnapsInCache(pid))
      ()
    }
  }

  private[this] def snaps(suffix: String): Future[driver.C] = {
    val snaps = driver.getSnaps(suffix)
    snaps.flatMap(_.indexesManager.ensure(Index(
      key = Seq((PROCESSOR_ID, IndexType.Ascending),
        (SEQUENCE_NUMBER, IndexType.Descending),
        (TIMESTAMP, IndexType.Descending)),
      name = Some(driver.snapsIndexName),
      unique = true,
      background = true,
      sparse = false,
      version = None,
      partialFilter = None,
      options = BSONDocument.empty
    )))
    snaps
  }

}
