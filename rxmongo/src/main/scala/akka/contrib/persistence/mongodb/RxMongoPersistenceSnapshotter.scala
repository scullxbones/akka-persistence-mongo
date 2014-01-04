package akka.contrib.persistence.mongodb

import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.api.indexes._
import reactivemongo.bson._
import reactivemongo.bson.buffer.ArrayReadableBuffer
import reactivemongo.core.commands._

import akka.persistence.SelectedSnapshot

import scala.collection.immutable.{Seq => ISeq}
import scala.concurrent._

import play.api.libs.iteratee.Iteratee

class RxMongoSnapshotter(driver: RxMongoPersistenceDriver) extends MongoPersistenceSnapshottingApi {
  
  import RxMongoPersistenceExtension._
  import SnapshottingFieldNames._
  
  implicit object SelectedSnapshotHandler extends BSONDocumentReader[SelectedSnapshot] with BSONDocumentWriter[SelectedSnapshot] {
    def read(doc: BSONDocument): SelectedSnapshot = {
      val content = doc.getAs[Array[Byte]](SERIALIZED).get
      driver.serialization.deserialize(content, classOf[SelectedSnapshot]).get
    }

    def write(snap: SelectedSnapshot): BSONDocument = {
      val content = driver.serialization.serialize(snap).get
      BSONDocument(PROCESSOR_ID -> snap.metadata.processorId,
        SEQUENCE_NUMBER -> snap.metadata.sequenceNr,
        TIMESTAMP -> snap.metadata.timestamp,
        SERIALIZED -> content)
    }
  }
  
  private[mongodb] def findYoungestSnapshotByMaxSequence(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = driver.breaker.withCircuitBreaker {
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
    driver.breaker.withCircuitBreaker(snaps.insert(snapshot).mapTo[Unit])
  
  private[mongodb] def deleteSnapshot(pid: String, seq: Long, ts: Long)(implicit ec: ExecutionContext) = driver.breaker.withCircuitBreaker {
    snaps.remove(BSONDocument(PROCESSOR_ID -> pid, SEQUENCE_NUMBER -> seq, TIMESTAMP -> ts))
  }
  
  private[mongodb] def deleteMatchingSnapshots(pid: String, maxSeq: Long, maxTs: Long)(implicit ec: ExecutionContext) = driver.breaker.withCircuitBreaker {
    snaps.remove(BSONDocument(PROCESSOR_ID -> pid, 
    						  SEQUENCE_NUMBER -> BSONDocument( "$lte" -> maxSeq ), 
    						  TIMESTAMP -> BSONDocument( "$lte" -> maxTs)))
  }
  
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
