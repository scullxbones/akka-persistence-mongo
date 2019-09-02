/*
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" migration tool
 * ...
 */
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
import scala.util.Try


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

    (if (settings.SuffixMigrationHeavyLoad) {
      buildTemporaryMapHeavyLoad(makeCollection, getNewCollectionName, originCollectionName, summaryTitle)
        .flatMap(_.grouped(settings.SuffixMigrationParallelism).foldLeft(Future.successful((0L, 0L, 0L, 0L, 0L))) {
          case (prevFuture, tmpMap) =>
            for {
              acc <- prevFuture
              res <- Future.fold(
                tmpMap.map { case (pid, count) =>
                  handleDocsByPid(pid, count, makeCollection, getNewCollectionName, originCollectionName, writeConcern)
                    .map { res => (res._1, res._2, res._3, res._4, count)}
                })((0L,0L,0L,0L,0L)){
                (accPid, resPid) => (accPid._1 + resPid._1, accPid._2 + resPid._2, accPid._3 + resPid._3, accPid._4 + resPid._4, accPid._5 + resPid._5)
              }
            } yield{
              (acc._1 + res._1, acc._2 + res._2, acc._3 + res._3, acc._4 + res._4, acc._5 + res._5)
            }
        })
    } else {
      buildTemporaryMap(makeCollection, getNewCollectionName, summaryTitle)
        .flatMap { tmpMap =>
          Future.fold(
            tmpMap.map {
              case (newCollectionName, (pids, count)) =>
                handleDocsByCollection(pids, count, makeCollection, originCollectionName, newCollectionName, writeConcern)
            }
          )((0L, 0L, 0L, 0L, 0L)){
            (acc, res) => (acc._1 + res._1, acc._2 + res._2, acc._3 + res._3, acc._4 + res._4, acc._5 + res._5)
          }
        }
    })
    .map { case (inserted, removed, failed, ignored, handled) =>

      logger.info(s"${summaryTitle.toUpperCase}: $handled records were handled")
      logger.info(s"${summaryTitle.toUpperCase}: $inserted/$handled records were successfully transferred to suffixed collections")
      logger.info(s"${summaryTitle.toUpperCase}: $removed/$handled records were successfully removed from '$originCollectionName' collection")
      if (ignored > 0L)
        logger.info(s"${summaryTitle.toUpperCase}: $ignored/$handled records were ignored and remain in '$originCollectionName' collection")
      if(removed < inserted)
        logger.warn(s"${summaryTitle.toUpperCase}: ${inserted - removed} records were transferred to suffixed collections but were NOT removed from '$originCollectionName'")
      if (failed > 0L)
        logger.error(s"${summaryTitle.toUpperCase}: $failed/$handled records lead to errors")

      // result
      failed + inserted - removed == 0L //OK if no error and removed = inserted
    }

  }

  //////////////// HEAVY LOAD OPERATIONS ///////////////

  private[this] val IgnoredPid = "IGNORED_PID"

  /**
    * Builds a Map counting documents per persistence Id: Map(pid -> count)
    */
  private[this] def buildTemporaryMapHeavyLoad(makeCollection: String => C, getNewCollectionName: String => String, originCollectionName: String, summaryTitle: String): Future[Map[String, Long]] = {
    logger.info(s"\n\n${summaryTitle.toUpperCase}: Gathering documents by suffixed collection names.  T h i s   m a y   t a k e   a   w h i l e  ! ! !   It may seem to freeze, be patient...\n")

    Source.fromFuture(makeCollection(""))
      .flatMapConcat(_.aggregate(List(group(s"$$$PROCESSOR_ID", sum("count", 1)))).asAkka)
      .runWith(Sink.fold[Map[String, Long], D](Map[String, Long]()) { case (tmpMap, tmpDoc) =>
        val count = tmpDoc.getInt32("count").getValue.toLong
        Option(tmpDoc.getString("_id").getValue).getOrElse("") match {
          case pid if pid.nonEmpty && getNewCollectionName(pid) != originCollectionName =>
            tmpMap + (pid -> count)
          case _ =>
            Try { tmpMap + (IgnoredPid -> (tmpMap(IgnoredPid) + count)) }
              .recover { case _: Throwable => tmpMap + (IgnoredPid -> count)}
              .get
        }
      })
  }

  /**
    * Migrates documents corresponding to one persistence Id from an origin collection to some new collection,
    * and returns a tuple containing amounts of documents inserted, removed, failed and ignored
    */
  private[this] def handleDocsByPid(pid: String, docCount: Long, makeCollection: String => C, getNewCollectionName: String => String, originCollectionName: String, writeConcern: WriteConcern): Future[(Long, Long, Long, Long)] = {
    if(pid == IgnoredPid) {
      Future.successful((0L, 0L, 0L, docCount)) // just counting...
    } else {
      logger.info(s"Processing persistence Id '$pid' for $docCount documents...")
      Source.fromFuture(makeCollection(""))
        .flatMapConcat(_.find(equal(PROCESSOR_ID, pid)).asAkka)
        .runWith(Sink.foldAsync[(Long, Long, Long), D]((0L, 0L, 0L)) { case ((insOk, delOk, ko), doc) =>
          val id = doc.getObjectId("_id")
          val idStr = id.getValue.toString
          Source.fromFuture(makeCollection(pid))
            .flatMapConcat(_.withWriteConcern(writeConcern).insertOne(doc).asAkka)
            .runWith(Sink.headOption)
            .flatMap {
              case Some(_) => // doc has been inserted, we remove it from origin collection...
                Source.fromFuture(makeCollection(""))
                  .flatMapConcat(_.withWriteConcern(writeConcern).deleteOne(equal("_id", id)).asAkka)
                  .runWith(Sink.headOption)
                  .flatMap {
                    case Some(delRes) if delRes.getDeletedCount == 1 =>
                      Future.successful((insOk + 1L, delOk + 1L, ko))
                    case _ =>
                      logger.warn(s"Document with unique id '$idStr' transferred to '${getNewCollectionName(pid)}' was NOT removed from '$originCollectionName'")
                      Future.successful((insOk + 1L, delOk, ko + 1L))
                  }
                  .recoverWith { case t: Throwable =>
                    logger.error(s"Unable to remove document with unique id '$idStr' from '$originCollectionName': ${t.getMessage}", t)
                    Future.successful((insOk + 1L, delOk, ko + 1L))
                  }
              case _ =>
                logger.warn(s"Document with unique id '$idStr' was NOT transferred to '${getNewCollectionName(pid)}' nor removed from '$originCollectionName'")
                Future.successful((insOk, delOk, ko + 1L))
            }
            .recoverWith { case t: Throwable =>
              logger.error(s"Unable to insert document with unique id '$idStr' into '${getNewCollectionName(pid)}': ${t.getMessage}", t)
              Future.successful((insOk, delOk, ko + 1L))
            }
        })
        .map { case (inserted, removed, failed) =>
          logger.info(s"Persistence Id '$pid' result: (inserted = $inserted, removed = $removed, failed = $failed)")
          (inserted, removed, failed, 0L)
        }
        .recoverWith { case t: Throwable =>
          logger.error(s"Unable to handle documents for persistence Id '$pid': ${t.getMessage}", t)
          Future.successful((0L, 0L, docCount, 0L))
        }
    }
  }

  //////////////// NORMAL OPERATIONS ///////////////

  /**
    * Builds a Map grouping persistence ids by new suffixed collection names: Map(collectionName -> (Seq[pid], count))
    */
  private[this] def buildTemporaryMap(makeCollection: String => C, getNewCollectionName: String => String, summaryTitle: String): Future[Map[String, (Seq[String], Long)]] = {
    logger.info(s"\n\n${summaryTitle.toUpperCase}: Gathering documents by suffixed collection names.  T h i s   m a y   t a k e   a   w h i l e  ! ! !   It may seem to freeze, be patient...\n")

    Source.fromFuture(makeCollection(""))
      .flatMapConcat(_.aggregate(List(group(s"$$$PROCESSOR_ID", sum("count", 1)))).asAkka)
      .runWith(Sink.seq)
      .map(_.groupBy(doc => getNewCollectionName(Option(doc.getString("_id").getValue).getOrElse(""))))
      .map(_.mapValues(_.foldLeft((Seq[String](), 0L)){ (acc, doc) =>
        (acc._1 :+ Option(doc.getString("_id").getValue).getOrElse(""), acc._2 + doc.getInt32("count").getValue)
      }).toMap)
  }

  /**
    * Migrates documents from an origin collection to some new collection, and returns a tuple containing amounts of documents inserted, removed, failed, ignored and handled.
    */
  private[this] def handleDocsByCollection(pids: Seq[String], docCount: Long, makeCollection: String => C, originCollectionName: String, newCollectionName: String, writeConcern: WriteConcern): Future[(Long, Long, Long, Long, Long)] = {
    if (originCollectionName == newCollectionName) {
      Future.successful((0L, 0L, 0L, docCount, docCount)) // just counting...
    } else {
      logger.info(s"Processing suffixed collection '$newCollectionName' for $docCount documents...")
      Future.fold(
        pids.map { pid =>
          Source.fromFuture(makeCollection(""))
            .flatMapConcat(_.find(equal(PROCESSOR_ID, pid)).asAkka)
            .runWith(Sink.seq)
            .flatMap(docs => insertManyDocs(docs, makeCollection, newCollectionName, writeConcern).map {res =>
              (res, docs.size.toLong - res, docs.size.toLong) // (inserted, failed, handled)
            })
            .flatMap { ins =>
              if (ins._1 > 0L) {
                removeManyDocs(pid, originCollectionName, writeConcern, ins._3).map { res =>
                  (ins._1, res, ins._2 + ins._3 - res, ins._3) // (inserted, removed, failed, handled)
                }
              } else {
                Future.successful((0L, 0L, ins._2, ins._3))
              }
            }
        })((0L, 0L, 0L, 0L)) { (acc, res) => (acc._1 + res._1, acc._2 + res._2, acc._3 + res._3, acc._4 + res._4)
      }
        .map {
          case (inserted, removed, failed, handled) =>

            logger.info(s"$handled records were handled for suffixed collection '$newCollectionName'")
            logger.info(s"$inserted/$handled records were successfully transferred to suffixed collection '$newCollectionName'")
            logger.info(s"$removed/$handled records, previously transferred to '$newCollectionName', were successfully removed from '$originCollectionName'")
            if(removed < inserted)
              logger.warn(s"${inserted - removed} records were transferred to suffixed collection '$newCollectionName' but were NOT removed from '$originCollectionName'")
            if (failed > 0L)
              logger.error(s"$failed/$handled records lead to errors while transferring from '$originCollectionName' to suffixed collection '$newCollectionName'")

            (inserted, removed, failed, 0L, handled)
        }
    }
  }

  /**
    * Inserts many documents in some new collection and returns the amount of inserted documents (zero in case of failure)
    */
  private[this] def insertManyDocs(docs: Seq[D], makeCollection: String => C, newCollectionName: String, writeConcern: WriteConcern, tryNb: Int = 1): Future[Long] = {
    Source.fromFuture(makeCollection(docs.head.getString(PROCESSOR_ID).getValue))
      .flatMapConcat(_.withWriteConcern(writeConcern).insertMany(docs).asAkka)
      .runWith(Sink.headOption)
      .flatMap {
        case Some(_) =>
          Future.successful(docs.size.toLong)
        case None if tryNb < settings.SuffixMigrationMaxInsertRetry || settings.SuffixMigrationMaxInsertRetry == 0 =>
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
  private[this] def removeManyDocs(pid: String, originCollectionName: String, writeConcern: WriteConcern, toRemove: Long, alreadyRemoved: Long = 0L, tryNb: Int = 1): Future[Long] = {
    Source.fromFuture(collection(originCollectionName))
      .flatMapConcat(_.withWriteConcern(writeConcern).deleteMany(equal(PROCESSOR_ID, pid)).asAkka)
      .runWith(Sink.headOption)
      .flatMap {
        case Some(delResult) if delResult.getDeletedCount == toRemove =>
          Future.successful(delResult.getDeletedCount + alreadyRemoved)
        case Some(delResult) if tryNb < settings.SuffixMigrationMaxDeleteRetry || settings.SuffixMigrationMaxDeleteRetry == 0 =>
          removeManyDocs(pid, originCollectionName, writeConcern, toRemove - delResult.getDeletedCount, alreadyRemoved + delResult.getDeletedCount, tryNb + 1)
        case None if tryNb < settings.SuffixMigrationMaxDeleteRetry || settings.SuffixMigrationMaxDeleteRetry == 0 =>
          removeManyDocs(pid, originCollectionName, writeConcern, toRemove, alreadyRemoved, tryNb + 1)
        case _ =>
          Future.successful(alreadyRemoved)
      }
      .recoverWith { case t: Throwable =>
        logger.error(s"Unable to remove all documents from '$originCollectionName' collection: ${t.getMessage}", t)
        Future.successful(alreadyRemoved)
      }
  }

  //////////////// COMMON OPERATIONS ///////////////

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
  private[this] def emptyMetadata(tryNb: Int = 1): Future[Unit] = {
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
              case Some(delResult) if tryNb < settings.SuffixMigrationMaxEmptyMetadataRetry || settings.SuffixMigrationMaxEmptyMetadataRetry == 0 =>
                logger.info(s"METADATA: ${delResult.getDeletedCount}/$count records only were successfully removed from '$metadataCollectionName' collection")
                emptyMetadata(tryNb + 1)
              case None if tryNb < settings.SuffixMigrationMaxEmptyMetadataRetry || settings.SuffixMigrationMaxEmptyMetadataRetry == 0 =>
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
