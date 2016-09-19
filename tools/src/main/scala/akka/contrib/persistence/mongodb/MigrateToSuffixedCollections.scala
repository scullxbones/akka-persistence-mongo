/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" migration tool
 * ...
 */
package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.casbah.Imports._
import com.typesafe.config.Config

import scala.util.Try
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import scala.util.Random

class MigrateToSuffixedCollections(system: ActorSystem, config: Config) extends CasbahMongoDriver(system, config) {

  def migrateToSuffixCollections(): Unit = {

    // INIT //

    logger.info("Starting automatic migration to collections with suffixed names\nThis may take a while...")

    if (settings.JournalAutomaticUpgrade) {
      logger.warn("Please, disable 'journal-automatic-upgrade' option when migrating from unique to suffixed collections. Aborting...")
      return
    }

    if (!useSuffixedCollectionNames) {
      logger.warn("Please, provide some 'suffix-builder.class' option when migrating from unique to suffixed collections. Aborting...")
      return
    }

    // create a temporary collection
    val temporaryCollectionName = s"migration2suffix-${System.currentTimeMillis()}-${Random.nextInt(1000)}"
    val temporaryCollection = collection(temporaryCollectionName)

    def pidQuery(persistenceId: String) = (PROCESSOR_ID $eq persistenceId)

    // JOURNALS //

    // aggregate persistenceIds, contained in unique journal, and store them in temporary collection
    journal.aggregate(
      MongoDBObject("$group" -> MongoDBObject("_id" -> s"$$$PROCESSOR_ID")) ::
        MongoDBObject("$out" -> temporaryCollectionName) ::
        Nil).results

    // for each retrieved persistenceId, bulk insert corresponding records from unique journal
    // to appropriate (and newly created) suffixed journal, then remove them from unique journal
    if (temporaryCollection.count() > 0) {
      temporaryCollection.find().toSeq.groupBy(tempDbObject => getJournalCollectionName(tempDbObject.get("_id").toString)).foldLeft(0) {
        case (n, (collectionName, tempDbObjects)) => {
          if (!collectionName.equals(settings.JournalCollection)) {
            val bulkInsert = journal(tempDbObjects.head.get("_id").toString).initializeOrderedBulkOperation
            tempDbObjects.foldLeft(0) {
              case (i, tdbo) =>
                val query = pidQuery(tdbo.get("_id").toString)
                val count = journal.count(query)
                journal.find(query) foreach bulkInsert.insert
                i + count
            } match {
              case ssTotalToInsert => {
                Try { bulkInsert.execute(journalWriteConcern) } map { wr =>
                  logger.info(s"${wr.getInsertedCount}/$ssTotalToInsert records were inserted into '$collectionName' journal")
                  tempDbObjects.foldLeft(0) {
                    case (i, tdbo) =>
                      val query = pidQuery(tdbo.get("_id").toString)
                      val count = journal.count(query)
                      Try { journal.remove(query, journalWriteConcern) } map { r =>
                        i + count
                      } recover {
                        case _: Throwable =>
                          logger.warn(s"Errors occurred when trying to remove records, previously copied to '$collectionName' journal, from '${settings.JournalCollection}' journal")
                          i
                      } getOrElse (i)
                  } match {
                    case ssTotalToRemove => {
                      logger.info(s"$ssTotalToRemove records, previously copied to '$collectionName' journal, were removed from '${settings.JournalCollection}' journal")
                      n + ssTotalToInsert
                    }
                  }
                } recover {
                  case _: Throwable =>
                    logger.warn(s"Errors occurred when trying to insert records into '$collectionName' journal")
                    n
                } getOrElse (n)
              }
            }
          } else {
            n
          }
        }
      } match {
        case total => logger.info(s"SUMMARY: $total records were successfully transfered to suffixed journals")
      }
    }


    // SNAPSHOTS //

    // aggregate persistenceIds, contained in unique snaps, and store them in temporary collection
    snaps.aggregate(
      MongoDBObject("$group" -> MongoDBObject("_id" -> s"$$$PROCESSOR_ID")) ::
        MongoDBObject("$out" -> temporaryCollectionName) ::
        Nil).results

    // for each retrieved persistenceId, bulk insert corresponding records from unique snaps
    // to appropriate (and newly created) suffixed snaps, then remove them from unique snaps
    if (temporaryCollection.count() > 0) {
      temporaryCollection.find().toSeq.groupBy(tempDbObject => getSnapsCollectionName(tempDbObject.get("_id").toString)).foldLeft(0) {
        case (n, (collectionName, tempDbObjects)) => {
          if (!collectionName.equals(settings.SnapsCollection)) {
            val bulkInsert = snaps(tempDbObjects.head.get("_id").toString).initializeOrderedBulkOperation
            tempDbObjects.foldLeft(0) {
              case (i, tdbo) =>
                val query = pidQuery(tdbo.get("_id").toString)
                val count = snaps.count(query)
                snaps.find(query) foreach bulkInsert.insert
                i + count
            } match {
              case ssTotalToInsert => {
                Try { bulkInsert.execute(snapsWriteConcern) } map { wr =>
                  logger.info(s"${wr.getInsertedCount}/$ssTotalToInsert records were inserted into '$collectionName' snapshot")
                  tempDbObjects.foldLeft(0) {
                    case (i, tdbo) =>
                      val query = pidQuery(tdbo.get("_id").toString)
                      val count = snaps.count(query)
                      Try { snaps.remove(query, snapsWriteConcern) } map { r =>
                        i + count
                      } recover {
                        case _: Throwable =>
                          logger.warn(s"Errors occurred when trying to remove records, previously copied to '$collectionName' snapshot, from '${settings.SnapsCollection}' snapshot")
                          i
                      } getOrElse (i)
                  } match {
                    case ssTotalToRemove => {
                      logger.info(s"$ssTotalToRemove records, previously copied to '$collectionName' snapshot, were removed from '${settings.SnapsCollection}' snapshot")
                      n + ssTotalToInsert
                    }
                  }
                } recover {
                  case _: Throwable =>
                    logger.warn(s"Errors occurred when trying to insert records into '$collectionName' snapshot")
                    n
                } getOrElse (n)
              }
            }
          } else {
            n
          }
        }
      } match {
        case total => logger.info(s"SUMMARY: $total records were successfully transfered to suffixed snapshots")
      }
    }


    // METADATA

    // Empty metadata collection, it will be rebuilt from suffixed collections through usual Akka persistence process
    val count = metadata.count()
    Try { metadata.remove(MongoDBObject(), metadataWriteConcern) } map {
      r => logger.info(s"SUMMARY: ${r.getN}/$count records were successfully removed from ${settings.MetadataCollection} collection") }recover {
      case t: Throwable => logger.warn(s"Trying to empty ${settings.MetadataCollection} collection failed.", t)
    }

    // CLEANING //

    Try(temporaryCollection.drop()) recover {
      case t: Throwable => logger.warn("No temporary collection to drop", t)
    }

    logger.info("Automatic migration to collections with suffixed names has completed")
  }
}