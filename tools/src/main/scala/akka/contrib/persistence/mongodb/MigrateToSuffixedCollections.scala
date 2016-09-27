/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" migration tool
 * ...
 */
package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.casbah.Imports._
import com.typesafe.config.Config

import akka.contrib.persistence.mongodb.JournallingFieldNames._

import scala.util.Try
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

    // JOURNALS // 

    handleMigration(journal)

    // SNAPSHOTS //  

    handleMigration(snaps)

    // METADATA //

    // Empty metadata collection, it will be rebuilt from suffixed collections through usual Akka persistence process
    val count = metadata.count()
    Try { metadata.remove(MongoDBObject(), metadataWriteConcern) } map {
      r => logger.info(s"METADATA: ${r.getN}/$count records were successfully removed from ${settings.MetadataCollection} collection")
    } recover {
      case t: Throwable => logger.error(s"Trying to empty ${settings.MetadataCollection} collection failed.", t)
    }

    // END //

    logger.info("Automatic migration to collections with suffixed names has completed")
  }

  /**
   * Applies migration process to some category, i.e. journal or snapshot
   */
  private[this] def handleMigration(originCollection: MongoCollection) = {

    val makeJournal: String => MongoCollection = journal
    val makeSnaps: String => MongoCollection = snaps

    // retrieve journal or snapshot properties
    val (makeNewCollection, getNewCollectionName, writeConcern, summaryTitle) = originCollection match {
      case c: MongoCollection if (c == journal) => (makeJournal, getJournalCollectionName(_), journalWriteConcern, "journals")
      case c: MongoCollection if (c == snaps)   => (makeSnaps, getSnapsCollectionName(_), snapsWriteConcern, "snapshots")
    }

    // create a temporary collection
    val temporaryCollectionName = s"migration2suffix-${System.currentTimeMillis()}-${Random.nextInt(1000)}"
    val temporaryCollection = collection(temporaryCollectionName)

    // aggregate persistenceIds, contained in unique collection, and store them in temporary collection (as "_id" field)
    originCollection.aggregate(
      MongoDBObject("$group" -> MongoDBObject("_id" -> s"$$$PROCESSOR_ID")) ::
        MongoDBObject("$out" -> temporaryCollectionName) ::
        Nil).results

    // for each retrieved persistenceId, insert corresponding records from unique collection
    // to appropriate (and newly created) suffixed collection, then remove them from unique collection
    if (temporaryCollection.count() > 0) {
      val totalCount = originCollection.count()
      // we group by future suffixed collection name, foldLeft methods are only here for counting
      val (totalOk, totalIgnored) = temporaryCollection.find().toSeq.groupBy(tempDbObject => getNewCollectionName(tempDbObject.get("_id").toString)).foldLeft(0L, 0L) {
        case ((done, ignored), (newCollectionName, tempDbObjects)) => {
          // we create suffixed collection
          val newCollection = makeNewCollection(tempDbObjects.head.get("_id").toString)
          // we check new suffixed collection is not origin unique collection
          if (!newCollectionName.equals(getOriginCollectionName(originCollection))) {
            // ok, we migrate records
            val migrated = migrateRecords(tempDbObjects, originCollection, newCollection, newCollectionName, writeConcern)
            (done + migrated, ignored)
          } else {
            // no migration but we count
            val notMigrated = ignoreRecords(tempDbObjects, originCollection)
            (done, ignored + notMigrated)
          }
        }
      }
      // logging...
      logger.info(s"${summaryTitle.toUpperCase}: $totalOk/$totalCount records were successfully transfered to suffixed collections")
      if (totalIgnored > 0) {
        logger.info(s"${summaryTitle.toUpperCase}: $totalIgnored/$totalCount records were ignored and remain in '${getOriginCollectionName(originCollection)}'")
        if (totalIgnored + totalOk == totalCount)
          logger.info(s"${summaryTitle.toUpperCase}: $totalOk + $totalIgnored = $totalCount, all records were successfully handled")
        else
          logger.warn(s"${summaryTitle.toUpperCase}: $totalOk + $totalIgnored does NOT equal $totalCount, check remaining records  in '${getOriginCollectionName(originCollection)}'")
      }
    }

    // remove temporary collection
    Try(temporaryCollection.drop()) recover {
      case t: Throwable => logger.warn("No temporary collection to drop", t)
    }

  }

  /**
   * migrate records from an origin collection to some new collection and returns the total amount of migrated records
   */
  private[this] def migrateRecords(tempDbObjects: Seq[DBObject],
                                   originCollection: MongoCollection,
                                   newCollection: MongoCollection,
                                   newCollectionName: String,
                                   writeConcern: WriteConcern): Long = {
    // first step: we insert records in new suffixed collection, foldLeft methods are only here for counting
    val (inserted, count) = tempDbObjects.foldLeft(0L, 0L) {
      case ((ok, tot), tdbo) =>
        val query = pidQuery(tdbo)
        val cnt = originCollection.count(query).toLong
        val ssTotOk = originCollection.find(query).foldLeft(0L)(insertIntoCollection(newCollection, newCollectionName, writeConcern))
        (ok + ssTotOk, tot + cnt)
    }
    logger.info(s"$inserted/$count records were inserted into '$newCollectionName'")

    // 2nd step: we remove records from unique collection
    val removed = tempDbObjects.foldLeft(0L)(removeFromCollection(originCollection, writeConcern))
    logger.info(s"$removed/$count records, previously copied to '$newCollectionName', were removed from '${getOriginCollectionName(originCollection)}'")
    if (removed < inserted) removed else inserted
  }

  /**
   * insert one record into some collection and returns the total amount of inserted records
   */
  private[this] def insertIntoCollection(newCollection: MongoCollection,
                                         newCollectionName: String,
                                         writeConcern: WriteConcern): (Long, DBObject) => Long = { (i, dbo) =>
    Try { newCollection.insert(dbo, writeConcern) } map { _ =>
      i + 1
    } recover {
      case _: Throwable =>
        logger.error(s"Errors occurred when trying to insert record in '$newCollectionName'")
        i
    } getOrElse (i)
  }

  /**
   * remove records from some collection and returns the total amount of removed records
   */
  private[this] def removeFromCollection(originCollection: MongoCollection,
                                         writeConcern: WriteConcern): (Long, DBObject) => Long = { (i, dbo) =>
    Try { originCollection.remove(pidQuery(dbo), writeConcern) } map { r =>
      i + r.getN
    } recover {
      case _: Throwable =>
        logger.error(s"Errors occurred when trying to remove records from '${getOriginCollectionName(originCollection)}'")
        i
    } getOrElse (i)

  }

  /**
   * count records ignored by migration process
   */
  private[this] def ignoreRecords(tempDbObjects: Seq[DBObject],
                                  originCollection: MongoCollection): Long = {
    val ignored = tempDbObjects.foldLeft(0L) {
      case (tot, tdbo) =>
        val query = pidQuery(tdbo)
        val cnt = originCollection.count(query).toLong
        tot + cnt
    }
    logger.info(s"$ignored records were ignored and remain in '${getOriginCollectionName(originCollection)}'")
    ignored
  }

  /**
   * Convenient method to generate a simple query widely used in this class
   */
  private[this] def pidQuery(dbo: DBObject) = (PROCESSOR_ID $eq dbo.get("_id").toString)

  /**
   * Convenient method to retrieve origin collection name
   */
  private[this] def getOriginCollectionName(originCollection: MongoCollection): String = originCollection match {
    case c: MongoCollection if (c == journal) => settings.JournalCollection
    case c: MongoCollection if (c == snaps)   => settings.SnapsCollection
  }

}