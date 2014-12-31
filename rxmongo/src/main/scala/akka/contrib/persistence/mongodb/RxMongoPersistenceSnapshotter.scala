package akka.contrib.persistence.mongodb

import reactivemongo.api.indexes._
import reactivemongo.bson._

import akka.persistence.SelectedSnapshot

import scala.concurrent._

class RxMongoSnapshotter(driver: RxMongoPersistenceDriver) extends MongoPersistenceSnapshottingApi {
  
  import RxMongoPersistenceExtension._
  import SnapshottingFieldNames._

  private[this] implicit val serialization = driver.serialization
  private[this] lazy val writeConcern = driver.snapsWriteConcern

  implicit object SelectedSnapshotHandler extends BSONDocumentReader[SelectedSnapshot] with BSONDocumentWriter[SelectedSnapshot] {
    def read(doc: BSONDocument): SelectedSnapshot = {
      val content = doc.getAs[Array[Byte]](SERIALIZED).get
      serialization.deserialize(content, classOf[SelectedSnapshot]).get
    }

    def write(snap: SelectedSnapshot): BSONDocument = {
      val content = serialization.serialize(snap).get
      BSONDocument(PROCESSOR_ID -> snap.metadata.persistenceId,
                    SEQUENCE_NUMBER -> snap.metadata.sequenceNr,
                    TIMESTAMP -> snap.metadata.timestamp,
                    SERIALIZED -> content)
    }
  }
  
  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = {
      val selected =
        snaps.find(
          BSONDocument(PROCESSOR_ID -> pid,
            SEQUENCE_NUMBER -> BSONDocument("$lte" -> maxSeq),
            TIMESTAMP -> BSONDocument("$lte" -> maxTs)
          )
        ).sort(BSONDocument(SEQUENCE_NUMBER -> -1, TIMESTAMP -> -1))
         .one[SelectedSnapshot]
      selected
    }

  private[mongodb] def saveSnapshot(snapshot: SelectedSnapshot)(implicit ec: ExecutionContext) = 
    snaps.insert(snapshot,writeConcern).map(_ => ())
  
  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) =
    snaps.remove(BSONDocument(PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq, TIMESTAMP -> ts),writeConcern)
  
  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) =
    snaps.remove(BSONDocument(PROCESSOR_ID -> pid, 
    						              SEQUENCE_NUMBER -> BSONDocument( "$lte" -> maxSeq ),
    						              TIMESTAMP -> BSONDocument( "$lte" -> maxTs)),
                 writeConcern)
  
  private[this] def snaps(implicit ec: ExecutionContext) = {
    val snaps = driver.collection(driver.snapsCollectionName)
    snaps.indexesManager.ensure(new Index(
      key = Seq((PROCESSOR_ID, IndexType.Ascending),
    		        (SEQUENCE_NUMBER, IndexType.Descending),
    		        (TIMESTAMP, IndexType.Descending)),
      background = true,
      unique = true,
      name = Some(driver.snapsIndexName)))
    snaps
  }
  
}
