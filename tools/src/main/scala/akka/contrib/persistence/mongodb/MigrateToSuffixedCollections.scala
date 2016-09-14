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

    logger.info("Starting automatic migration to collections with suffixed names.\nThis may take a while...")

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
      temporaryCollection.find().foreach { dbObject =>
        dbObject.getAs[String]("_id") match {
          case Some(persistenceId) if (!getJournalCollectionName(persistenceId).equals(settings.JournalCollection)) => {

            val tryInsert = Try {
              val bulkInsert = journal(persistenceId).initializeOrderedBulkOperation
              journal.find(pidQuery(persistenceId)) foreach bulkInsert.insert
              bulkInsert.execute(journalWriteConcern)
            }
            if (tryInsert.isSuccess) {
              logger.debug(s"Journal suffix migration: inserting '$persistenceId' records in '${getJournalCollectionName(persistenceId)}' completed successfully")
              Try {
                journal.remove(pidQuery(persistenceId), journalWriteConcern)
                logger.debug(s"Journal suffix migration: removing '$persistenceId' records from '${settings.JournalCollection}' completed successfully")
              } recover {
                case t: Throwable =>
                  logger.error(s"Journal suffix migration: removing '$persistenceId' records from '${settings.JournalCollection}' did NOT complete successfully", t)
              }

            } else {
              logger.error(s"Journal suffix migration: inserting '$persistenceId' records in '${getJournalCollectionName(persistenceId)}' did NOT complete successfully")
            }
          }

          case Some(persistenceId) if (getJournalCollectionName(persistenceId).equals(settings.JournalCollection)) =>
            logger.warn(s"Journal suffix migration: inserting '$persistenceId' records in '${getJournalCollectionName(persistenceId)}' ignored")

          case _ =>
            logger.warn(s"Journal suffix migration: record without '_id' field encountered in temporary collection")
        }
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
      temporaryCollection.find().foreach { dbObject =>
        dbObject.getAs[String]("_id") match {
          case Some(persistenceId) if (!getSnapsCollectionName(persistenceId).equals(settings.SnapsCollection)) => {

            val tryInsert = Try {
              val bulkInsert = snaps(persistenceId).initializeOrderedBulkOperation
              snaps.find(pidQuery(persistenceId)) foreach bulkInsert.insert
              bulkInsert.execute(snapsWriteConcern)
            }
            if (tryInsert.isSuccess) {
              logger.debug(s"Snapshot suffix migration: inserting '$persistenceId' records in '${getSnapsCollectionName(persistenceId)}' completed successfully")
              Try {
                snaps.remove(pidQuery(persistenceId), snapsWriteConcern)
                logger.debug(s"Snapshot suffix migration: removing '$persistenceId' records from '${settings.SnapsCollection}' completed successfully")
              } recover {
                case t: Throwable =>
                  logger.error(s"Snapshot suffix migration: removing '$persistenceId' records from '${settings.SnapsCollection}' did NOT complete successfully", t)
              }

            } else {
              logger.error(s"Snapshot suffix migration: inserting '$persistenceId' records in '${getSnapsCollectionName(persistenceId)}' did NOT complete successfully")
            }
          }

          case Some(persistenceId) if (getSnapsCollectionName(persistenceId).equals(settings.SnapsCollection)) =>
            logger.warn(s"Snapshot suffix migration: inserting '$persistenceId' records in '${getSnapsCollectionName(persistenceId)}' ignored")

          case _ =>
            logger.warn(s"Snapshot suffix migration: record without '_id' field encountered in temporary collection")
        }
      }
    }

    // CLEANING //

    Try(temporaryCollection.drop()) recover {
      case t: Throwable => logger.warn("No temporary collection to drop", t)
    }

    logger.info("Automatic migration to collections with suffixed names has completed.\nYou are all good if no ERROR message appeared.")
  }
}