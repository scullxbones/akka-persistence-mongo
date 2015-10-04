package akka.contrib.persistence.mongodb

import com.typesafe.config.Config
import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.bson.{BSONDropIndexesImplicits, BSONListIndexesImplicits}
import reactivemongo.core.nodeset.Authenticate

import reactivemongo.api.commands._
import reactivemongo.api.indexes.{IndexType, Index}
import reactivemongo.bson._

import akka.actor.ActorSystem

import scala.concurrent.{Awaitable, Future, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object RxMongoPersistenceDriver {
  import MongoPersistenceDriver._

  def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean):WriteConcern = (writeSafety,wtimeout.toMillis.toInt,fsync) match {
    case (Unacknowledged,wt,f) =>
      WriteConcern.Unacknowledged.copy(fsync = f, wtimeout = Option(wt))
    case (Acknowledged,wt,f) =>
      WriteConcern.Acknowledged.copy(fsync = f, wtimeout = Option(wt))
    case (Journaled,wt,_) =>
      WriteConcern.Journaled.copy(wtimeout = Option(wt))
    case (ReplicaAcknowledged,wt,f) =>
      WriteConcern.ReplicaAcknowledged(2, wt, !f)
  }
}

class RxMongoDriver(system: ActorSystem, config: Config) extends MongoPersistenceDriver(system, config) {
  import RxMongoPersistenceDriver._
  import concurrent.Await
  import concurrent.duration._

  // Collection type
  type C = BSONCollection

  type D = BSONDocument

  private[mongodb] lazy val driver = MongoDriver()
  private[this] lazy val parsedMongoUri = MongoConnection.parseURI(mongoUri) match {
    case Success(parsed) => parsed
    case Failure(throwable) => throw throwable
  }
  private[this] lazy val unauthenticatedConnection =
    // create unauthenticated connection, there is no direct way to wait for authentication this way
    // plus prevent sending double authentication (initial authenticate and our explicit authenticate)
    waitForPrimary(driver.connection(parsedURI = parsedMongoUri.copy(authenticate = None)))
  private[mongodb] lazy val connection =
    // now authenticate explicitly and wait for confirmation
    parsedMongoUri.authenticate.fold(unauthenticatedConnection) { auth =>
      waitForAuthentication(unauthenticatedConnection, auth)
    }

  implicit val waitFor = 4.seconds
  private[this] def waitForPrimary(conn: MongoConnection): MongoConnection = {
    wait(conn.waitForPrimary(waitFor minus 1.seconds))
    conn
  }
  private[this] def waitForAuthentication(conn: MongoConnection, auth: Authenticate): MongoConnection = {
    wait(conn.authenticate(auth.db, auth.user, auth.password))
    conn
  }
  private[this] def wait[T](awaitable: Awaitable[T])(implicit duration: Duration): T =
    Await.result(awaitable, duration)

  def walk(collection: BSONCollection)(previous: Future[Seq[WriteResult]], doc: BSONDocument)(implicit ec: ExecutionContext): Cursor.State[Future[Seq[WriteResult]]] = {
    import scala.collection.immutable.{Seq => ISeq}
    import RxMongoSerializers._
    import DefaultBSONHandlers._
    import Producer._

    val id = doc.getAs[BSONObjectID]("_id").get
    val ev = deserializeJournal(doc)
    val q = BSONDocument("_id" -> id)

    // Wait for previous record to be updated
    val wr = previous.flatMap(wrs =>
      collection.update(q, serializeJournal(Atom(ev.pid, ev.sn, ev.sn, ISeq(ev)))).map(wrs :+ _)
    )

    Cursor.Cont(wr)
  }

  override private[mongodb] def upgradeJournalIfNeeded(): Unit = {
    import concurrent.ExecutionContext.Implicits.global
    import JournallingFieldNames._

    val j = collection(journalCollectionName)
    val walker = walk(j) _
    val q = BSONDocument(VERSION -> BSONDocument("$exists" -> 0))
    val empty: Future[Seq[WriteResult]] = Future.successful(DefaultWriteResult(
      ok = true, n = 0,
      writeErrors = Seq.empty, writeConcernError = None,
      code = None, errmsg = None
    ) :: Nil)

    def traverse(count: Int) = {
      logger.info(s"Journal automatic upgrade found $count records needing upgrade")
      if (count > 0) {
        j.find(q).cursor[BSONDocument]().foldWhile(empty)(walker, (_,t) => Cursor.Fail(t)).flatMap(identity)
      } else empty
    }

    val eventuallyUpgrade = for {
      _ <- j.remove(BSONDocument(PROCESSOR_ID -> BSONRegex("^/user/sharding/[^/]+Coordinator/singleton/coordinator","")))
            .map(wr => logger.info(s"Successfully removed ${wr.n} legacy cluster sharding records"))
            .recover { case t => logger.error(s"Error while removing legacy cluster sharding records",t) }
      indices <- j.indexesManager.list()
      _ <- indices
              .find(_.key.sortBy(_._1) == Seq(DELETED -> IndexType.Ascending, PROCESSOR_ID -> IndexType.Ascending, SEQUENCE_NUMBER -> IndexType.Ascending))
              .map(_.eventualName)
              .map(n => j.indexesManager.drop(n).transform(
                _ => logger.info("Successfully dropped legacy index"),
                { t =>
                  logger.error("Error received while dropping legacy index",t)
                  t
                }
              ))
              .getOrElse(Future.successful(()))
      count <- j.count(Option(q))
      wr <- traverse(count)
    } yield wr

    eventuallyUpgrade.onComplete {
      case Success(wrs) if wrs.exists(w => w.inError || w.hasErrors) =>
        val errors = wrs.filter(_.inError).map(r => s"${r.code} - ${r.message}").mkString("\n")
        logger.error("Upgrade did not complete successfully")
        logger.error(s"Errors during journal auto-upgrade:\n$errors")
        val writeErrors = wrs.filter(_.hasErrors).flatMap(_.writeErrors).map(we => s"${we.code} - ${we.errmsg}").mkString("\n")
        logger.error(s"Received ${wrs.count(_.hasErrors)} write errors during journal auto-upgrade:\n$writeErrors")
      case Success(wrs) =>
        val successCount = wrs.foldLeft(0)((sum,wr) => sum + wr.n)
        logger.info(s"Successfully upgraded $successCount records")
      case Failure(t) =>
        logger.error(s"Upgrade did not complete successfully",t)
    }

    Await.result(eventuallyUpgrade, 2.minutes) // ouch

    ()
  }

  private[mongodb] def closeConnections(): Unit = driver.close()

  private[mongodb] def dbName: String = parsedMongoUri.db.getOrElse(DEFAULT_DB_NAME)
  private[mongodb] def db = connection(dbName)(system.dispatcher)

  private[mongodb] override def collection(name: String) = db[BSONCollection](name)
  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety,journalWTimeout,journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety,snapsWTimeout,snapsFsync)

  private[mongodb] override def ensureUniqueIndex(collection: C, indexName: String, keys: (String,Int)*)(implicit ec: ExecutionContext) = {
    val ky = keys.toSeq.map{ case (f,o) => f -> (if (o > 0) IndexType.Ascending else IndexType.Descending)}
    collection.indexesManager.ensure(new Index(
      key = ky,
      background = true,
      unique = true,
      name = Some(indexName)))
    collection
  }
}

class RxMongoPersistenceExtension(actorSystem: ActorSystem) extends MongoPersistenceExtension {

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {
    val driver = new RxMongoDriver(actorSystem, config)

    override lazy val journaler = new RxMongoJournaller(driver) with MongoPersistenceJournalMetrics with MongoPersistenceJournalFailFast {
      override def driverName = "rxmongo"
      override private[mongodb] val breaker = driver.breaker
    }

    override lazy val snapshotter = new RxMongoSnapshotter(driver) with MongoPersistenceSnapshotFailFast {
      override private[mongodb] val breaker = driver.breaker
    }
    override lazy val readJournal = new RxMongoReadJournaller(driver)
  }

}
