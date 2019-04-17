package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.contrib.persistence.mongodb.RxStreamsInterop._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.mongodb.scala.WriteConcern
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Aggregates.{group, out}
import org.mongodb.scala.model.Filters.equal

import scala.concurrent.Future
import scala.util.Random


class ScalaDriverMigrateToSuffixedCollections(system: ActorSystem) extends ScalaMongoDriver(system, ConfigFactory.empty()) {

  implicit val mat: ActorMaterializer = ActorMaterializer()

  private final val MaxInsertRetry = 5
  private final val MaxRemoveRetry = 5
  private final val MaxEmptyMetadataRetry = 5

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
        Future.failed(new RuntimeException("Automatic migration to collections with suffixed names has failed"))
      }
    }
  }

  private[this] def handleMigration(originCollectionName: String): Future[Boolean] = {

    val makeJournal: String => C = journal
    val makeSnaps: String => C = snaps

    // retrieve journal or snapshot properties
    val (makeNewCollection, getNewCollectionName, writeConcern, summaryTitle) = originCollectionName match {
      case str: String if str == journalCollectionName => (makeJournal, getJournalCollectionName _, journalWriteConcern, "journals")
      case str: String if str == snapsCollectionName => (makeSnaps, getSnapsCollectionName _, snapsWriteConcern, "snapshots")
    }

    buildTemporaryMap(originCollectionName, getNewCollectionName)
    .flatMap { tmpMap =>
      Future.fold(
        tmpMap.map {
          case (newCollectionName, pids) =>
            handleDocs(pids, originCollectionName, newCollectionName, writeConcern, summaryTitle)
        }
      )((0L, 0L, 0L, 0L, 0L)){
        (acc, res) => (acc._1 + res._1, acc._2 + res._2, acc._3 + res._3, acc._4 + res._4, acc._5 + res._5)
      }
    }
    .map { case (inserted, removed, ignored, errors, total) =>

      logger.info(s"${summaryTitle.toUpperCase}: $inserted/$total records were successfully transferred to suffixed collections")
      logger.info(s"${summaryTitle.toUpperCase}: $removed/$total records were successfully removed from '$originCollectionName' collection")
      if (ignored > 0L)
        logger.info(s"${summaryTitle.toUpperCase}: $ignored/$total records were ignored and remain in '$originCollectionName' collection")
      if (errors > 0L)
        logger.error(s"${summaryTitle.toUpperCase}: $errors/$total records lead to errors")
      errors == 0L
    }

  }

  /**
    * Builds a Map grouping persistence ids by new suffixed collection names
    */
  private[this] def buildTemporaryMap(originCollectionName: String, getNewCollectionName: String => String): Future[Map[String, Seq[String]]] = {
    val temporaryCollectionName = s"migration2suffix-${System.currentTimeMillis()}-${Random.nextInt(1000)}"
    var dummyId = 0L // dummy ids are used to count 'ignored' in case pid is null or empty
    Source.fromFuture(collection(originCollectionName))
      .flatMapConcat(_.aggregate(List(group(s"$$$PROCESSOR_ID"), out(temporaryCollectionName))).asAkka)
      .runWith(Sink.seq)
      .map(_.groupBy(doc => getNewCollectionName(Option(doc.getString("_id").getValue).getOrElse(""))))
      .map(_.mapValues(_.map(doc => Option(doc.getString("_id").getValue).getOrElse(s"${dummyId += 1}"))))
      .map { res =>
        Source.fromFuture(collection(temporaryCollectionName))
          .flatMapConcat(_.drop().asAkka)
          .runWith(Sink.ignore)
        res
      }
  }

  /**
    * returns (inserted, removed, ignored, errors, total)
    */
  private[this] def handleDocs(pids: Seq[String], originCollectionName: String, newCollectionName: String, writeConcern: WriteConcern, summaryTitle: String): Future[(Long, Long, Long, Long, Long)] = {
    if (originCollectionName == newCollectionName) {
      logger.info(s"${summaryTitle.toUpperCase}: ${pids.size.toLong}/${pids.size.toLong} records were ignored and remain in '$originCollectionName' collection")
      Future.successful((0L, 0L, pids.size.toLong, 0L, pids.size.toLong))
    } else {
      Future.fold(
        pids.map { pid =>
          Source.fromFuture(collection(originCollectionName))
            .flatMapConcat(_.find(equal(PROCESSOR_ID, pid)).asAkka)
            .runWith(Sink.seq)
            .flatMap(docs => insertManyDocs(docs, newCollectionName, writeConcern).map {res =>
              (res, docs.size.toLong - res, docs.size.toLong) // (inserted, errors, total)
            })
            .flatMap(ins => removeManyDocs(pid, originCollectionName, writeConcern).map { res =>
              (ins._1, res, ins._2 + ins._3 - res ,ins._3) // (inserted, removed, errors, total)
            })
        })((0L, 0L, 0L, 0L)) { (acc, res) => (acc._1 + res._1, acc._2 + res._2, acc._3 + res._3, acc._4 + res._4)
      }
      .map {
        case (inserted, removed, errors, total) =>

          logger.info(s"${summaryTitle.toUpperCase}: $inserted/$total records were successfully transferred to suffixed collection '$newCollectionName'")
          logger.info(s"${summaryTitle.toUpperCase}: $removed/$total records, previously copied to '$newCollectionName', were successfully removed from '$originCollectionName'")
          if (errors > 0L)
            logger.error(s"${summaryTitle.toUpperCase}: $errors/$total records lead to errors while transferring from '$originCollectionName' to suffixed collection '$newCollectionName'")

          (inserted, removed, 0L, errors, total)
      }
    }
  }

  /**
    * returns inserted
    */
  private[this] def insertManyDocs(docs: Seq[D], newCollectionName: String, writeConcern: WriteConcern, tryNb: Int = 0): Future[Long] = {
    Source.fromFuture(collection(newCollectionName))
      .flatMapConcat(_.withWriteConcern(writeConcern).insertMany(docs).asAkka)
      .runWith(Sink.headOption)
      .flatMap {
        case Some(_) => Future.successful(docs.size.toLong)
        case None if tryNb < MaxInsertRetry => insertManyDocs(docs, newCollectionName, writeConcern, tryNb + 1)
        case _ => Future.successful(0L)
      }
      .recoverWith { case t: Throwable =>
        logger.error(s"Unable to insert documents into '$newCollectionName' collection: ${t.getMessage}", t)
        Future.successful(0L)
      }
  }

  /**
    * returns removed
    */
  private[this] def removeManyDocs(pid: String, originCollectionName: String, writeConcern: WriteConcern, tryNb: Int = 0): Future[Long] = {
    Source.fromFuture(collection(originCollectionName))
      .flatMapConcat(_.withWriteConcern(writeConcern).deleteMany(equal(PROCESSOR_ID, pid)).asAkka)
      .runWith(Sink.headOption)
      .flatMap {
        case Some(deleteResult) => Future.successful(deleteResult.getDeletedCount)
        case None if tryNb < MaxRemoveRetry => removeManyDocs(pid, originCollectionName, writeConcern, tryNb + 1)
        case _ => Future.successful(0L)
      }
      .recoverWith { case t: Throwable =>
        logger.error(s"Unable to remove documents from '$originCollectionName' collection: ${t.getMessage}", t)
        Future.successful(0L)
      }
  }

  private[this] def checkJournalAutomaticUpgrade: Future[Unit] = {
    if (settings.JournalAutomaticUpgrade) {
      val errorMsg = "Please, disable 'journal-automatic-upgrade' option when migrating from unique to suffixed collections. Aborting..."
      logger.warn(errorMsg)
      Future.failed(new RuntimeException(errorMsg))
    } else Future.successful(())
  }

  private[this] def checkUseSuffixedCollectionNames: Future[Unit] = {
    if (!useSuffixedCollectionNames) {
      val errorMsg = "Please, provide some 'suffix-builder.class' option when migrating from unique to suffixed collections. Aborting..."
      logger.warn(errorMsg)
      Future.failed(new RuntimeException(errorMsg))
    } else Future.successful(())
  }

  /**
    * Empty metadata collection, it will be rebuilt from suffixed collections through usual Akka persistence process
    */
  private[this] def emptyMetadata(tryNb: Int = 0): Future[Unit] = {
    Source.fromFuture(collection(metadataCollectionName))
      .flatMapConcat(_.withWriteConcern(metadataWriteConcern).deleteMany(Document()).asAkka) // TODO: check that deleteMany(Document()) really deletes all documents from a collection
      .runWith(Sink.headOption)
      .flatMap {
        case Some(delResult) => // ok
          Future.successful(logger.info(s"METADATA: ${delResult.getDeletedCount} records were successfully removed from '$metadataCollectionName' collection"))
        case None if tryNb < MaxEmptyMetadataRetry =>
          emptyMetadata(tryNb + 1)
        case _ =>
          val warnMsg = s"METADATA: Unable to remove all records from '$metadataCollectionName' collection"
          logger.warn(warnMsg)
          Future.failed(new RuntimeException(warnMsg))
      }
      .recover {
        case t: Throwable =>
          logger.error(s"Trying to empty '$metadataCollectionName' collection failed.", t)
          throw t
      }
  }

}
