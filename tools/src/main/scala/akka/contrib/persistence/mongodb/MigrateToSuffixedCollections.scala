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

class MigrateToSuffixedCollections(system: ActorSystem, config: Config) extends CasbahMongoDriver(system, config) {

  def migrateToSuffixCollections(): Unit = {
    import akka.contrib.persistence.mongodb.JournallingFieldNames._
    import scala.util.Random

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

    // for each retrieved persistenceId, insert corresponding records from unique journal
    // to appropriate (and newly created) suffixed journal, then remove them from unique journal
    if (temporaryCollection.count() > 0) {
      val totalCount = journal.count()
      temporaryCollection.find().toSeq.groupBy(tempDbObject => getJournalCollectionName(tempDbObject.get("_id").toString)).foldLeft(0L) {
        case (n, (collectionName, tempDbObjects)) => {
          if (!collectionName.equals(settings.JournalCollection)) {
            val newCollection = journal(tempDbObjects.head.get("_id").toString)
            tempDbObjects.foldLeft(0L, 0L) {
              case ((ok, tot), tdbo) =>
                val query = pidQuery(tdbo.get("_id").toString)
                val cnt = journal.count(query).toLong
                journal.find(query).foldLeft(0L) {
                  case (i, dbo) =>
                    Try { newCollection.insert(dbo, journalWriteConcern) } map { _ =>
                      i + 1
                    } recover {
                      case _: Throwable =>
                        logger.warn(s"Errors occurred when trying to insert record in '$collectionName' journal")
                        i
                    } getOrElse (i)
                } match { case ssTotOk => (ok + ssTotOk, tot + cnt) }
            } match {
              case (inserted, count) =>
                logger.info(s"$inserted/$count records were inserted into '$collectionName' journal")
                tempDbObjects.foldLeft(0L) {
                  case (ok, tdbo) =>
                    val query = pidQuery(tdbo.get("_id").toString)
                    Try { journal.remove(query, journalWriteConcern) } map { r =>
                      ok + r.getN
                    } recover {
                      case _: Throwable =>
                        logger.warn(s"Errors occurred when trying to remove records, previously copied to '$collectionName' journal, from '${settings.JournalCollection}' journal")
                        ok
                    } getOrElse (ok)
                } match {
                  case removed =>
                    logger.info(s"$removed/$count records, previously copied to '$collectionName' journal, were removed from '${settings.JournalCollection}' journal")
                    if (removed < inserted) n + removed else n + inserted
                }
            }
          } else n
        }
      } match { case totalOk => logger.info(s"JOURNALS: $totalOk/$totalCount records were successfully transfered to suffixed collections") }
    }

    // SNAPSHOTS //

    // aggregate persistenceIds, contained in unique snaps, and store them in temporary collection
    snaps.aggregate(
      MongoDBObject("$group" -> MongoDBObject("_id" -> s"$$$PROCESSOR_ID")) ::
        MongoDBObject("$out" -> temporaryCollectionName) ::
        Nil).results

    // for each retrieved persistenceId, insert corresponding records from unique snaps
    // to appropriate (and newly created) suffixed snaps, then remove them from unique snaps
    if (temporaryCollection.count() > 0) {
      val totalCount = snaps.count()
      temporaryCollection.find().toSeq.groupBy(tempDbObject => getSnapsCollectionName(tempDbObject.get("_id").toString)).foldLeft(0L) {
        case (n, (collectionName, tempDbObjects)) => {
          if (!collectionName.equals(settings.SnapsCollection)) {
            val newCollection = snaps(tempDbObjects.head.get("_id").toString)
            tempDbObjects.foldLeft(0L, 0L) {
              case ((ok, tot), tdbo) =>
                val query = pidQuery(tdbo.get("_id").toString)
                val cnt = snaps.count(query).toLong
                snaps.find(query).foldLeft(0L) {
                  case (i, dbo) =>
                    Try { newCollection.insert(dbo, snapsWriteConcern) } map { _ =>
                      i + 1
                    } recover {
                      case _: Throwable =>
                        logger.warn(s"Errors occurred when trying to insert record in '$collectionName' snapshot")
                        i
                    } getOrElse (i)
                } match { case ssTotOk => (ok + ssTotOk, tot + cnt) }
            } match {
              case (inserted, count) =>
                logger.info(s"$inserted/$count records were inserted into '$collectionName' snapshot")
                tempDbObjects.foldLeft(0L) {
                  case (ok, tdbo) =>
                    val query = pidQuery(tdbo.get("_id").toString)
                    Try { snaps.remove(query, snapsWriteConcern) } map { r =>
                      ok + r.getN
                    } recover {
                      case _: Throwable =>
                        logger.warn(s"Errors occurred when trying to remove records, previously copied to '$collectionName' snapshot, from '${settings.JournalCollection}' snapshot")
                        ok
                    } getOrElse (ok)
                } match {
                  case removed =>
                    logger.info(s"$removed/$count records, previously copied to '$collectionName' snapshot, were removed from '${settings.JournalCollection}' snapshot")
                    if (removed < inserted) n + removed else n + inserted
                }
            }
          } else n
        }
      } match { case totalOk => logger.info(s"SNAPSHOTS: $totalOk/$totalCount records were successfully transfered to suffixed collections") }
    }
    

    // METADATA

    // Empty metadata collection, it will be rebuilt from suffixed collections through usual Akka persistence process
    val count = metadata.count()
    Try { metadata.remove(MongoDBObject(), metadataWriteConcern) } map {
      r => logger.info(s"METADATA: ${r.getN}/$count records were successfully removed from ${settings.MetadataCollection} collection")
    } recover {
      case t: Throwable => logger.warn(s"Trying to empty ${settings.MetadataCollection} collection failed.", t)
    }

    // CLEANING //

    Try(temporaryCollection.drop()) recover {
      case t: Throwable => logger.warn("No temporary collection to drop", t)
    }

    logger.info("Automatic migration to collections with suffixed names has completed")
  }
}