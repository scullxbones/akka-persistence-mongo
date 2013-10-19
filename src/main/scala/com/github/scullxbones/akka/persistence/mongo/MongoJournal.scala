package com.github.scullxbones.akka.persistence.mongo

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import akka.persistence._
import akka.persistence.journal._
import akka.persistence.snapshot._
import akka.actor._
import akka.serialization._
import com.typesafe.config.Config
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands._
import play.api.libs.iteratee.Iteratee

class MongoJournalExtension(actorSystem: ExtendedActorSystem) extends Extension {
  
  private val config = actorSystem.settings.config
  
  lazy val mongoUrl = config.getString("mongo-journal.url")
  lazy val mongoDbName = config.getString("mongo-journal.db")
  
  val driver = new MongoDriver
  
  lazy val connection = driver.connection(List(mongoUrl))
  lazy val db = connection(mongoDbName)
  
  lazy val journal = db("akka-persistence-journal")
  
  object AkkaPersistence {
    implicit object ActorRefReader extends BSONDocumentReader[ActorRef] {
      def read(doc: BSONDocument): ActorRef =
        actorSystem.provider.resolveActorRef(doc.getAs[String]("p").get)
    }

    implicit object ActorRefWriter extends BSONDocumentWriter[ActorRef] {
      def write(ref: ActorRef): BSONDocument =
        BSONDocument("p" -> Serialization.serializedActorPath(ref))
    }
  }

  object MongoJournal {

    import AkkaPersistence._

    implicit object PersistentImplReader extends BSONDocumentReader[PersistentImpl] {
      def read(doc: BSONDocument): PersistentImpl =
        PersistentImpl(
          payload = doc.getAs[String]("pd"),
          sequenceNr = doc.getAs[Long]("sq").get,
          processorId = doc.getAs[String]("pid").get,
          channelId = doc.getAs[String]("cid").get,
          deleted = doc.getAs[Boolean]("dl").get,
          resolved = doc.getAs[Boolean]("rv").get,
          confirms = doc.getAs[Seq[String]]("cs").get,
          sender = doc.getAs[ActorRef]("sn").get,
          confirmTarget = doc.getAs[ActorRef]("tgt").get)
    }

    implicit object PersistentImplWriter extends BSONDocumentWriter[PersistentImpl] {

      def write(persistent: PersistentImpl): BSONDocument =
        BSONDocument("pd" -> persistent.payload.toString,
          "sq" -> persistent.sequenceNr,
          "rv" -> persistent.resolved,
          "pid" -> persistent.processorId,
          "cid" -> persistent.channelId,
          "sn" -> persistent.sender,
          "dl" -> persistent.deleted,
          "cs" ->  BSONArray(persistent.confirms),
          "tgt" -> persistent.confirmTarget)
    }
    
    private[this] def journalEntry(pid: String, seq: Long) = 
      BSONDocument("pid" -> pid, "sn" -> seq)
      
    private[this] def journalRange(pid: String, from: Long, to: Long) =
      BSONDocument("pid" -> pid, "sn" -> BSONDocument( "$gte" -> from, "$lte" -> to))

	def appendToJournal(impl: PersistentImpl) = journal.insert(impl)
	
	def modifyJournalEntry(pid: String, seq: Long, op: BSONDocument) =
	  journal.db.command(new FindAndModify(journal.name, journalEntry(pid, seq), Update(op, false)))

	def markJournalDeleted(processorId: String, sequenceNr: Long) =
	  modifyJournalEntry(processorId, sequenceNr, BSONDocument("$set" -> BSONDocument("dl"->true)))
	
	def appendConfirm(processorId: String,sequenceNr: Long, confirm: String) =
	  modifyJournalEntry(processorId, sequenceNr, BSONDocument("$push" -> BSONDocument("cs"->confirm)))
	  
	def replay(pid: String, from: Long, to: Long)(cb: (PersistentImpl) => Unit) = {
      val cursor = journal.find(journalRange(pid,from,to)).cursor[PersistentImpl]
      val result:Future[Long] = cursor.enumerate.apply(Iteratee.foreach { p =>
        cb(p)
      }).flatMap(_ => { 
        val max = Aggregate(journal.name, 
            Seq(Match(BSONDocument("pid" -> pid)),
                GroupField("pid")( ("sn", Max("sn") ) )
                )
            )
        journal.db.command(max)
      }).map(x => x.iterator.next().getAs[Long]("result").getOrElse(0) )
      result
    }
  }
}

object MongoJournalExtensionId extends ExtensionId[MongoJournalExtension] {

  def lookup = MongoJournalExtensionId

  override def createExtension(actorSystem: ExtendedActorSystem) =
    new MongoJournalExtension(actorSystem)

  override def get(actorSystem: ActorSystem) = super.get(actorSystem)
}

class MongoJournal extends AsyncWriteJournal {
  
  private val extension = context.system.extension(MongoJournalExtensionId)
  
  import extension.MongoJournal._
  
  /**
   * Plugin API.
   *
   * Asynchronously writes a `persistent` message to the journal.
   */
  def writeAsync(persistent: PersistentImpl): Future[Unit] = 
    appendToJournal(persistent).mapTo[Unit]

  /**
   * Plugin API.
   *
   * Asynchronously marks a `persistent` message as deleted.
   */
  def deleteAsync(persistent: PersistentImpl): Future[Unit] =
    markJournalDeleted(persistent.processorId,persistent.sequenceNr).mapTo[Unit]

  /**
   * Plugin API.
   *
   * Asynchronously writes a delivery confirmation to the journal.
   */
  def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = 
    appendConfirm(processorId,sequenceNr,channelId).mapTo[Unit]

  /**
   * Plugin API.
   *
   * Asynchronously replays persistent messages. Implementations replay a message
   * by calling `replayCallback`. The returned future must be completed when all
   * messages (matching the sequence number bounds) have been replayed. The future
   * `Long` value must be the highest stored sequence number in the journal for the
   * specified processor. The future must be completed with a failure if any of
   * the persistent messages could not be replayed.
   *
   * The `replayCallback` must also be called with messages that have been marked
   * as deleted. In this case a replayed message's `deleted` field must be set to
   * `true`.
   *
   * The channel ids of delivery confirmations that are available for a replayed
   * message must be contained in that message's `confirms` sequence.
   *
   * @param processorId processor id.
   * @param fromSequenceNr sequence number where replay should start.
   * @param toSequenceNr sequence number where replay should end (inclusive).
   * @param replayCallback called to replay a single message.
   *
   * @see [[AsyncWriteJournal]]
   * @see [[SyncWriteJournal]]
   */
  def replayAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: PersistentImpl ⇒ Unit): Future[Long] = {
    replay(processorId, fromSequenceNr, toSequenceNr)(replayCallback)
  }
}