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

//    for {
//      tempMap <- buildTemporaryMap(originCollectionName, getNewCollectionName)
//      x <- Future.fold(
//        tempMap.map {
//          case (newCollectionName, pids) =>
//              handleDocs(pids, originCollectionName, newCollectionName, writeConcern, summaryTitle)
//        }
//      )((0L, 0L, 0L)){
//        (acc, res) => (acc._1 + res._1, acc._2 + res._2, acc._3 + res._3)
//      }
//    } yield x

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
    .map { res =>
      logger.info(s"${summaryTitle.toUpperCase}: ${res._1}/${res._5} records were successfully transferred to suffixed collections")
      logger.info(s"${summaryTitle.toUpperCase}: ${res._2}/${res._5} records were successfully removed from '$originCollectionName'")
      logger.info(s"${summaryTitle.toUpperCase}: ${res._3}/${res._5} records were ignored and remain in '$originCollectionName'")
      logger.error(s"${summaryTitle.toUpperCase}: ${res._4}/${res._5} records lead to errors")
      res._4 == 0L
    }


//    Source.fromFuture(collection(originCollectionName))
//      .flatMapConcat(_.find().asAkka)
//      .runWith(Sink.foldAsync[(Map[String, (Long, Long)], Long), D]((Map[String, (Long, Long)](), 0L)){ // accumulator: (Map(collName -> (success, error)), ignored))
//        case (acc, doc) =>
//          Option(doc.getString(PROCESSOR_ID)) match {
//            case Some(bStr) if bStr.getValue != null =>
//              getNewCollectionName(bStr.getValue) match {
//                case `originCollectionName` => // ignored
//                  Future.successful((acc._1, acc._2 + 1))
//                case newCollectionName =>
//                  for {
//                    res1 <- insertDoc(makeNewCollection, newCollectionName, writeConcern, bStr.getValue, doc)
//                    res2 <- removeDoc(originCollectionName, writeConcern, bStr.getValue)
//                  } yield {
//                    if (res1 && res2) {
//                      acc._1.get(newCollectionName) match {
//                        case Some(tup) => (acc._1 + ((newCollectionName, (tup._1 + 1L, tup._2))), acc._2)
//                        case None => (acc._1 + ((newCollectionName, (1L, 0L))), acc._2)
//                      }
//                    } else {
//                      acc._1.get(newCollectionName) match {
//                        case Some(tup) => (acc._1 + ((newCollectionName, (tup._1, tup._2 + 1L))), acc._2)
//                        case None => (acc._1 + ((newCollectionName, (0L, 1L))), acc._2)
//                      }
//                    }
//                  }
//              }
//            case _ => // no persistenceId in this document, so we ignore it
//              Future.successful((acc._1, acc._2 + 1))
//          }
//      })
//      .map { res =>
//        // logging...
//        val totalOk = res._1.values.map(_._1).fold(0L)(_ + _)
//        val totalError = res._1.values.map(_._2).fold(0L)(_ + _)
//        val totalIgnored = res._2
//        val totalCount = totalOk + totalIgnored + totalError
//        val collectionsInError = res._1.foldLeft(Set[String]()){
//          case (acc, kv) if kv._2._2 > 0 => acc + kv._1
//        }
//        logger.info(s"${summaryTitle.toUpperCase}: $totalOk/$totalCount records were successfully transfered to suffixed collections")
//        logger.info(s"${summaryTitle.toUpperCase}: $totalIgnored/$totalCount records were ignored and remain in '$originCollectionName'")
//        logger.warn(s"${summaryTitle.toUpperCase}: $totalError/$totalCount records were NOT successfully transfered to suffixed collections")
//        logger.warn(s"${summaryTitle.toUpperCase}: suffixed collections for which problem occurred are the following: ${collectionsInError.mkString(", ")}")
//        // result
//        totalError == 0
//      }
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
      logger.info(s"${summaryTitle.toUpperCase}: ${pids.size.toLong}/${pids.size.toLong} records were ignored and remain in '$originCollectionName'")
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
        logger.error(s"Unable to insert documents into collection '$newCollectionName': ${t.getMessage}", t)
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
        logger.error(s"Unable to remove documents from '$originCollectionName': ${t.getMessage}", t)
        Future.successful(0L)
      }
  }

//  /**
//    * @return true if success, false otherwise, never fail.
//    */
//  private[this] def insertDoc(makeNewCollection: String => C, collectionName: String, writeConcern: WriteConcern, pid: String, doc: D, tryNb: Int = 0): Future[Boolean] = {
//    Source.fromFuture(makeNewCollection(pid))
//      .flatMapConcat(_.withWriteConcern(writeConcern).insertOne(doc).asAkka)
//      .runWith(Sink.headOption)
//      .flatMap {
//        case Some(_) => // ok
//          Future.successful(true)
//        case None if tryNb < MaxInsertRetry => // stream completes before signaling at least a single element
//          insertDoc(makeNewCollection, collectionName, writeConcern, pid, doc, tryNb + 1)
//        case _ =>
//          logger.warn(s"Unable to insert document in collection $collectionName for persistence Id $pid")
//          Future.successful(false)
//      }
//      .recoverWith { case t: Throwable =>
//        logger.error(s"Unable to insert document in collection $collectionName for persistence Id $pid: ${t.getMessage}", t)
//        Future.successful(false)
//      }
//  }
//
//  /**
//    * @return true if success, false otherwise, never fail.
//    */
//  private[this] def removeDoc(originCollectionName: String, writeConcern: WriteConcern, pid: String, tryNb: Int = 0): Future[Boolean] = {
//    Source.fromFuture(collection(originCollectionName))
//      .flatMapConcat(_.withWriteConcern(writeConcern).deleteOne(equal(PROCESSOR_ID, pid)).asAkka)
//      .runWith(Sink.headOption)
//      .flatMap {
//        case Some(_) => // ok
//          Future.successful(true)
//        case None if tryNb < MaxRemoveRetry => // stream completes before signaling at least a single element
//          removeDoc(originCollectionName, writeConcern, pid, tryNb + 1)
//        case _ =>
//          logger.warn(s"Unable to remove document from collection $originCollectionName for persistence Id $pid")
//          Future.successful(false)
//      }
//      .recoverWith { case t: Throwable =>
//        logger.error(s"Unable to remove document from collection $originCollectionName for persistence Id $pid: ${t.getMessage}", t)
//        Future.successful(false)
//      }
//  }

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
          Future.successful(logger.info(s"METADATA: ${delResult.getDeletedCount} records were successfully removed from $metadataCollectionName collection"))
        case None if tryNb < MaxEmptyMetadataRetry => // stream completes before signaling at least a single element
          emptyMetadata(tryNb + 1)
        case _ =>
          val warnMsg = s"METADATA: Unable to remove all records from $metadataCollectionName collection"
          logger.warn(warnMsg)
          Future.failed(new RuntimeException(warnMsg))
      }
      .recover {
        case t: Throwable =>
          logger.error(s"Trying to empty $metadataCollectionName collection failed.", t)
          throw t
      }

//    (for {
//      metadataColl <- collection(metadataCollectionName)
//      count <- metadataColl.countDocuments().toFuture()
//      // TODO: check that deleteMany(Document()) really deletes all documents of the collection
//      deletedCount <- metadataColl.withWriteConcern(metadataWriteConcern).deleteMany(Document()).toFuture()
//    } yield {
//      logger.info(s"METADATA: $deletedCount/$count records were successfully removed from ${settings.MetadataCollection} collection")
//    }) recover {
//      case t: Throwable => logger.error(s"Trying to empty ${settings.MetadataCollection} collection failed.", t)
//    }
  }

}
