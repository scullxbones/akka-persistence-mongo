package com.github.scullxbones.mongka

import org.scalatest.FunSpec
import com.github.simplyscala.MongoEmbedDatabase
import akka.actor.ActorSystem
import akka.persistence.Processor
import akka.persistence.Persistent
import akka.persistence.PersistenceFailure
import akka.actor.Props
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.ShouldMatchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike
import akka.persistence.RecoveryFailure

object MongkaJournalSpec {
  class MyEventSourcer extends Processor {
	
    val successes = new AtomicInteger(0)
    val failures = new AtomicInteger(0)
    
    def receive = {
      case Persistent(payload, sequenceNr) ⇒ {
        sender ! successes.incrementAndGet()
      }
      case PersistenceFailure(payload, sequenceNr, cause) ⇒ {
        sender ! failures.incrementAndGet()
      }
      case RecoveryFailure(cause) ⇒ {
        cause.printStackTrace()
      }
      case other ⇒ {
        println(s"Received message ${other}")
        //unhandled(other)
      }
    }
  }
}

@RunWith(classOf[JUnitRunner])
class MongkaJournalSpec (_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with FunSpecLike with BeforeAndAfterAll with MongoEmbedDatabase with ShouldMatchers {

  def this() = this(ActorSystem("MongkaJournalSpec"))
  
  import MongkaJournalSpec._
  
  override def afterAll = TestKit.shutdownActorSystem(_system)
  
  describe("an eventsourcing journal") {

    val actor = _system.actorOf(Props[MyEventSourcer])
    
    it("should log persistent messages") {
      actor ! Persistent("test1")
      expectMsg(1)
      actor ! Persistent("test2")
      expectMsg(2)
      actor ! Persistent("test3")
      expectMsg(3)
      actor ! Persistent("test4")
      expectMsg(4)
    }

  }

}


