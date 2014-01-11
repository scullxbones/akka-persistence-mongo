package akka.contrib.persistence.mongodb

import com.mongodb.casbah.Imports._
import akka.persistence.Persistent
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import akka.persistence.PersistentRepr
import akka.pattern.CircuitBreaker
import akka.actor.Scheduler
import scala.language.postfixOps
import com.mongodb.WriteConcern
import akka.serialization.Serialization
import scala.util.Success
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.testkit.TestKit
import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import java.util.{List => JList}
import scala.collection.immutable.{Seq => ISeq}
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceJournallerSpec extends TestKit(ActorSystem("unit-test")) with BaseUnitTest with EmbedMongo with BeforeAndAfter {

  import org.mockito.Mockito._
  import org.mockito.Matchers._
  import CasbahPersistenceJournaller._
  import JournallingFieldNames._
  import MongoMatchers._
  
  implicit val callerRuns = new ExecutionContext {
	  def reportFailure(t: Throwable) { t.printStackTrace() }
      def execute(runnable: Runnable) {runnable.run()}
  }
  
  implicit val serialization = SerializationExtension(system)
  
  def embeddedCollection(name: String) = mongoDB(name)

  val scheduler = mock[Scheduler]
  val driver = new CasbahPersistenceDriver {
    val actorSystem = system
    override lazy val breaker = CircuitBreaker(scheduler,0,10 seconds, 10 seconds)
    override def collection(name: String) = embeddedCollection(name)
  }

  val underTest = new CasbahPersistenceJournaller(driver)
  
  val records = List(1,2,3).map { sq => PersistentRepr(payload = "payload", sequenceNr = sq, processorId = "unit-test") }
  
  def withJournal(testCode: MongoCollection => Any) = {
	val journal = mongoDB(driver.journalCollectionName)
	try {
	  testCode(journal)
	} finally {
	  journal.dropCollection
	}
  }
  
  "A mongo journal implementation" should "serialize and deserialize non-confirmable data" in {
    
    val repr = PersistentRepr(payload = "TEST", sequenceNr = 1, processorId = "pid")
    
    val serialized = serializeJournal(repr)
    
    serialized(PROCESSOR_ID) should be ("pid")
    serialized(DELETED) should === (false)
    serialized(SEQUENCE_NUMBER) should be (1)
    
    val deserialized = deserializeJournal(serialized)
    
    deserialized.payload should be ("TEST")
    deserialized.processorId should be ("pid")
    deserialized.deleted should === (false)
    deserialized.sequenceNr should be (1)
    
  }
  
  it should "serialize and deserialize confirmable data" in {
    val repr = PersistentRepr(payload = "TEST", sequenceNr = 1, processorId = "pid", confirmable = true, confirms = ISeq("uno"))
    
    val serialized = serializeJournal(repr)
    
    serialized(PROCESSOR_ID) should be ("pid")
    serialized(DELETED) should === (false)
    serialized(SEQUENCE_NUMBER) should be (1)
    
    serialized(CONFIRMS).asInstanceOf[Seq[_]] should have size 1
    serialized(CONFIRMS).asInstanceOf[Seq[_]] should contain ("uno")
    
    val deserialized = deserializeJournal(serialized)
    
    deserialized.payload should be ("TEST")
    deserialized.processorId should be ("pid")
    deserialized.deleted should === (false)
    deserialized.sequenceNr should be (1)
    deserialized.confirms should contain ("uno")
  }
  
  it should "create an appropriate index" in withJournal { journal =>
    underTest.journal
    
    val idx = journal.getIndexInfo.filter(obj => obj("name").equals(driver.journalIndexName)).head
    idx("unique") should === (true)
    idx("key") should be (MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1, DELETED -> 1))
  }
  
  it should "insert journal records" in withJournal { journal =>
    underTest.appendToJournal(records)

    journal.size should be(3)

    val recone = journal.head
    recone(PROCESSOR_ID) should be("unit-test")
    recone(SEQUENCE_NUMBER) should be(1)
    recone(DELETED) should === (false)
    recone(CONFIRMS).asInstanceOf[JList[_]] shouldBe empty
  }
  
  it should "hard delete journal entries" in withJournal { journal =>
	journal.insert(records: _*)
	
	underTest.deleteJournalEntries("unit-test", 1, 2, true)
	
	journal.size should be(1)
	val recone = journal.head
    recone(PROCESSOR_ID) should be("unit-test")
    recone(SEQUENCE_NUMBER) should be(3)
    recone(DELETED) should === (false)
  }

  it should "soft delete journal entries" in withJournal { journal =>
	journal.insert(records: _*)
	
	underTest.deleteJournalEntries("unit-test", 1, 2, false)
	
	journal.size should be(3)
	val recone = journal.head
    recone(PROCESSOR_ID) should be("unit-test")
    recone(SEQUENCE_NUMBER) should be(1)
    recone(DELETED) should === (true)
    
    val rectwo = journal.findOne(MongoDBObject(SEQUENCE_NUMBER -> 2)).get
    rectwo(SEQUENCE_NUMBER) should be(2)
	rectwo(DELETED) should === (true)
    
    val recthree = journal.findOne(MongoDBObject(SEQUENCE_NUMBER -> 3)).get
    recthree(SEQUENCE_NUMBER) should be(3)
	recthree(DELETED) should === (false)
  }
  
  it should "confirm journal entries" in withJournal { journal => 
	journal.insert(records: _*)

 	underTest.confirmJournalEntry("unit-test", 1, "4chan")
	List(1,2,3).foreach { sq =>
		underTest.confirmJournalEntry("unit-test", sq, "1chan")
	 	underTest.confirmJournalEntry("unit-test", sq, "2chan")
	 	underTest.confirmJournalEntry("unit-test", sq, "3chan")
	}
	
	val consone :: constwo :: consthree :: _ = List(1,2,3).map { sq =>
	  journal.findOne(MongoDBObject(SEQUENCE_NUMBER -> sq)).get(CONFIRMS).asInstanceOf[JList[String]].asScala
	}
	
	consone should contain inOrder ("4chan","1chan","2chan","3chan")
	constwo should contain only ("1chan","2chan","3chan")
	consthree should contain only ("1chan","2chan","3chan")
  }
  
  it should "replay journal entries" in withJournal { journal =>
    journal.insert(records: _*)
    
    var buf = Buffer[PersistentRepr]()
    val result = underTest.replayJournal("unit-test", 2, 3)(buf += _).value.get.get
    
    result should be (3)
    buf should have size 2
    buf should contain (PersistentRepr(payload = "payload", sequenceNr = 2, processorId = "unit-test"))
  }
}