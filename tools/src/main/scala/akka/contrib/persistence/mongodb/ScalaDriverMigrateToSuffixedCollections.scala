package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.contrib.persistence.mongodb.RxStreamsInterop._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.mongodb.scala.WriteConcern
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters.equal

import scala.concurrent.Future
import scala.util.Random


class ScalaDriverMigrateToSuffixedCollections(system: ActorSystem) extends ScalaMongoDriver(system, ConfigFactory.empty()) {

  implicit val mat: ActorMaterializer = ActorMaterializer()

  /**
    * Performs migration from unique collection to multiple collections with suffixed names
    */
  def migrateToSuffixCollections: Future[Unit] = {

    logger.info("Starting automatic migration to collections with suffixed names\nThis may take a while...")

    for {
      _ <- checkJournalAutomaticUpgrade
      _ <- checkUseSuffixedCollectionNames
      res1 <- handleMigration(journalCollectionName)
      res2 <- handleMigration(snapsCollectionName)
      _ <- emptyMetadata()
    } yield {
      if (res1 && res2) {
        logger.info("Automatic migration to collections with suffixed names has completed")
      } else {
        throw new RuntimeException("Automatic migration to collections with suffixed names has failed")
      }
    }
  }

  /**
    * Applies migration process to some category, i.e. journal or snapshot
    */
  private[this] def handleMigration(originCollectionName: String): Future[Boolean] = {

    val makeJournal: String => C = journal
    val makeSnaps: String => C = snaps

    // retrieve journal or snapshot properties
    val (makeCollection, getNewCollectionName, writeConcern, summaryTitle) = originCollectionName match {
      case str: String if str == journalCollectionName => (makeJournal, getJournalCollectionName _, journalWriteConcern, "journals")
      case str: String if str == snapsCollectionName => (makeSnaps, getSnapsCollectionName _, snapsWriteConcern, "snapshots")
    }

    buildTemporaryMap(makeCollection, getNewCollectionName)
    .flatMap { tmpMap =>
      Future.fold(
        tmpMap.map {
          case (newCollectionName, (pids, count)) =>
            handleDocs(pids, count, makeCollection, originCollectionName, newCollectionName, writeConcern, summaryTitle)
        }
      )((0L, 0L, 0L, 0L, 0L, 0L)){
        (acc, res) => (acc._1 + res._1, acc._2 + res._2, acc._3 + res._3, acc._4 + res._4, acc._5 + res._5, acc._6 + res._6)
      }
    }
    .map { case (inserted, removed, ignored, failed, handled, total) =>

      logger.info(s"${summaryTitle.toUpperCase}: $handled/$total records were handled")
      logger.info(s"${summaryTitle.toUpperCase}: $inserted/$total records were successfully transferred to suffixed collections")
      logger.info(s"${summaryTitle.toUpperCase}: $removed/$total records were successfully removed from '$originCollectionName' collection")
      if (ignored > 0L)
        logger.info(s"${summaryTitle.toUpperCase}: $ignored/$total records were ignored and remain in '$originCollectionName' collection")
      if(removed < inserted)
        logger.warn(s"${summaryTitle.toUpperCase}: ${inserted - removed} records were transferred to suffixed collections but were NOT removed from '$originCollectionName'")
      if (failed > 0L)
        logger.error(s"${summaryTitle.toUpperCase}: $failed/$total records lead to errors")

      // result
      failed + inserted - removed == 0L //OK if no error and removed = inserted
    }

  }

  /**
    * Builds a Map grouping persistence ids by new suffixed collection names: Map(collectionName -> (Seq[pid], count))
    */
  private[this] def buildTemporaryMap(makeCollection: String => C, getNewCollectionName: String => String): Future[Map[String, (Seq[String], Long)]] = {
    val temporaryCollectionName = s"migration2suffix-${System.currentTimeMillis()}-${Random.nextInt(1000)}"
    Source.fromFuture(makeCollection(""))
      .flatMapConcat(_.aggregate(List(group(s"$$$PROCESSOR_ID", sum("count", 1)), out(temporaryCollectionName))).asAkka)
      .runWith(Sink.seq)
      .map(_.groupBy(doc => getNewCollectionName(Option(doc.getString("_id").getValue).getOrElse(""))))
      .map(_.mapValues(_.foldLeft((Seq[String](), 0L)){ (acc, doc) =>
        (acc._1 :+ Option(doc.getString("_id").getValue).getOrElse(""), acc._2 + doc.getInt32("count").getValue)
      }))
      .map { res =>
        Source.fromFuture(collection(temporaryCollectionName))
          .flatMapConcat(_.drop().asAkka)
          .runWith(Sink.ignore)
        res
      }
  }

  /**
    * Migrates documents from an origin collection to some new collection, and returns a tuple containing amounts of documents inserted, removed, ignored, failed, handled and initial count.
    */
  private[this] def handleDocs(pids: Seq[String], count: Long, makeCollection: String => C, originCollectionName: String, newCollectionName: String, writeConcern: WriteConcern, summaryTitle: String): Future[(Long, Long, Long, Long, Long, Long)] = {
    if (originCollectionName == newCollectionName) {
      logger.info(s"${summaryTitle.toUpperCase}: $count/$count records were ignored and remain in '$originCollectionName' collection")
      Future.successful((0L, 0L, count, 0L, count, count))
    } else {
      Future.fold(
        pids.map { pid =>
          Source.fromFuture(makeCollection(""))
            .flatMapConcat(_.find(equal(PROCESSOR_ID, pid)).asAkka)
            .runWith(Sink.seq)
            .flatMap(docs => insertManyDocs(docs, makeCollection, newCollectionName, writeConcern).map {res =>
              (res, docs.size.toLong - res, docs.size.toLong) // (inserted, failed, handled)
            })
            .flatMap(ins => removeManyDocs(pid, originCollectionName, writeConcern, ins._3).map { res =>
              (ins._1, res, ins._2 + ins._3 - res, ins._3) // (inserted, removed, failed, handled)
            })
        })((0L, 0L, 0L, 0L)) { (acc, res) => (acc._1 + res._1, acc._2 + res._2, acc._3 + res._3, acc._4 + res._4)
      }
      .map {
        case (inserted, removed, failed, handled) =>

          logger.info(s"${summaryTitle.toUpperCase}: $handled/$count records were handled for suffixed collection '$newCollectionName'")
          logger.info(s"${summaryTitle.toUpperCase}: $inserted/$count records were successfully transferred to suffixed collection '$newCollectionName'")
          logger.info(s"${summaryTitle.toUpperCase}: $removed/$count records, previously copied to '$newCollectionName', were successfully removed from '$originCollectionName'")
          if(removed < inserted)
            logger.warn(s"${summaryTitle.toUpperCase}: ${inserted - removed} records were transferred to suffixed collection '$newCollectionName' but were NOT removed from '$originCollectionName'")
          if (failed > 0L)
            logger.error(s"${summaryTitle.toUpperCase}: $failed/$count records lead to errors while transferring from '$originCollectionName' to suffixed collection '$newCollectionName'")

          (inserted, removed, 0L, failed, handled, count)
      }
    }
  }

  /**
    * Inserts many documents in some new collection and returns the amount of inserted documents (zero in case of failure)
    */
  private[this] def insertManyDocs(docs: Seq[D], makeCollection: String => C, newCollectionName: String, writeConcern: WriteConcern, tryNb: Int = 0): Future[Long] = {
    Source.fromFuture(makeCollection(docs.head.getString(PROCESSOR_ID).getValue))
      .flatMapConcat(_.withWriteConcern(writeConcern).insertMany(docs).asAkka)
      .runWith(Sink.headOption)
      .flatMap {
        case Some(_) =>
          Future.successful(docs.size.toLong)
        case None if tryNb < settings.SuffixMigrationMaxInsertRetry =>
          insertManyDocs(docs, makeCollection, newCollectionName, writeConcern, tryNb + 1)
        case _ =>
          Future.successful(0L)
      }
      .recoverWith { case t: Throwable =>
        logger.error(s"Unable to insert documents into '$newCollectionName' collection: ${t.getMessage}", t)
        Future.successful(0L)
      }
  }

  /**
    * Removes many documents from an origin collection and returns the amount of removed documents (zero in case of total failure)
    */
  private[this] def removeManyDocs(pid: String, originCollectionName: String, writeConcern: WriteConcern, toRemove: Long, alreadyRemoved: Long = 0L, tryNb: Int = 0): Future[Long] = {
    Source.fromFuture(collection(originCollectionName))
      .flatMapConcat(_.withWriteConcern(writeConcern).deleteMany(equal(PROCESSOR_ID, pid)).asAkka)
      .runWith(Sink.headOption)
      .flatMap {
        case Some(delResult) if delResult.getDeletedCount == toRemove =>
          Future.successful(delResult.getDeletedCount + alreadyRemoved)
        case Some(delResult) if tryNb < settings.SuffixMigrationMaxRemoveRetry =>
          removeManyDocs(pid, originCollectionName, writeConcern, toRemove - delResult.getDeletedCount, alreadyRemoved + delResult.getDeletedCount, tryNb + 1)
        case None if tryNb < settings.SuffixMigrationMaxRemoveRetry =>
          removeManyDocs(pid, originCollectionName, writeConcern, toRemove, alreadyRemoved, tryNb + 1)
        case _ =>
          Future.successful(alreadyRemoved)
      }
      .recoverWith { case t: Throwable =>
        logger.error(s"Unable to remove all documents from '$originCollectionName' collection: ${t.getMessage}", t)
        Future.successful(alreadyRemoved)
      }
  }

  /**
    * Fails if 'journal-automatic-upgrade' property is set
    */
  private[this] def checkJournalAutomaticUpgrade: Future[Unit] = {
    if (settings.JournalAutomaticUpgrade) {
      val errorMsg = "Please, disable 'journal-automatic-upgrade' option when migrating from unique to suffixed collections. Aborting..."
      logger.warn(errorMsg)
      Future.failed(new RuntimeException(errorMsg))
    } else Future.successful(())
  }

  /**
    * Fails if 'suffix-builder' properties are not set
    */
  private[this] def checkUseSuffixedCollectionNames: Future[Unit] = {
    if (!useSuffixedCollectionNames) {
      val errorMsg = "Please, provide some 'suffix-builder.class' option when migrating from unique to suffixed collections. Aborting..."
      logger.warn(errorMsg)
      Future.failed(new RuntimeException(errorMsg))
    } else Future.successful(())
  }

  /**
    * Empties metadata collection, it will be rebuilt from suffixed collections through usual Akka persistence process
    */
  private[this] def emptyMetadata(tryNb: Int = 0): Future[Unit] = {
    Source.fromFuture(collection(metadataCollectionName))
      .flatMapConcat(_.countDocuments(Document()).asAkka)
      .runWith(Sink.head)
      .flatMap { count =>
        if (count > 0L) {
          Source.fromFuture(collection(metadataCollectionName))
            .flatMapConcat(_.withWriteConcern(metadataWriteConcern).deleteMany(Document()).asAkka)
            .runWith(Sink.headOption)
            .flatMap {
              case Some(delResult) if delResult.getDeletedCount == count =>
                Future.successful(logger.info(s"METADATA: all $count records were successfully removed from '$metadataCollectionName' collection"))
              case Some(delResult) if tryNb < settings.SuffixMigrationMaxEmptyMetadataRetry =>
                logger.info(s"METADATA: ${delResult.getDeletedCount}/$count records only were successfully removed from '$metadataCollectionName' collection")
                emptyMetadata(tryNb + 1)
              case None if tryNb < settings.SuffixMigrationMaxEmptyMetadataRetry =>
                emptyMetadata(tryNb + 1)
              case _ =>
                val warnMsg = s"METADATA: Unable to remove all records from '$metadataCollectionName' collection"
                logger.warn(warnMsg)
                Future.failed(new RuntimeException(warnMsg))
            }
        } else {
          Future.successful(())
        }
      }
      .recover {
        case t: Throwable =>
          logger.error(s"Trying to empty '$metadataCollectionName' collection failed.", t)
          throw t
      }
  }

}
