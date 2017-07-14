package akka.contrib.persistence.mongodb

import akka.actor.{ActorLogging, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.Tagged

object TestStubActors {
  object Counter {
    def props = Props(new Counter)
    case object Inc
    case object GetCounter
    case object Shutdown
  }

  class Counter extends PersistentActor{
    import Counter._
    var counter = 0

    override def receiveRecover: Receive = {
      case Inc => counter += 1
      case x:Int => counter += x
    }

    override def receiveCommand: Receive = {
      case Inc =>
        persist(1) { _ =>
          counter += 1
        }
      case GetCounter => sender() ! counter
      case Shutdown => context stop self
    }

    override def persistenceId: String = self.path.name
  }

  object Taggarific {
    def props(persistenceId: String) = Props(new Taggarific(persistenceId))

    case class TaggedEvent(s: String, tags: Set[String])
    case object Persisted
    case object Shutdown
  }

  class Taggarific(override val persistenceId: String) extends PersistentActor with ActorLogging {
    import Taggarific._

    override def receiveRecover: Receive = {
      case _ =>
    }

    override def receiveCommand: Receive = {
      case TaggedEvent(s, tags) if tags.nonEmpty =>
        persist(Tagged(s, tags)){_ =>
          sender() ! Persisted
        }
      case TaggedEvent(s, _) =>
        persist(s){_ =>
          sender() ! Persisted
        }
      case Shutdown =>
        context stop self
    }

    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(cause, s"Persistence failed for pid $persistenceId, sn $seqNr, event $event")
    }

    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(cause, s"Persistence rejected for pid $persistenceId, sn $seqNr, event $event")
    }
  }
}
