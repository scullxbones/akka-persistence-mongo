package akka.contrib.persistence.mongodb

import akka.testkit.TestKit

import scala.concurrent.ExecutionContext

trait MongoPersistenceSpec[D,C] extends BaseUnitTest with EmbeddedMongo { self: TestKit =>

  implicit val callerRuns = new ExecutionContext {
    def reportFailure(t: Throwable) { t.printStackTrace() }
    def execute(runnable: Runnable) { runnable.run() }
  }

  def driver:D

  def withCollection(name: String)(testCode: C => Any):Any

  def withJournal(testCode: C => Any):Any

  def withSnapshot(testCode: C => Any):Any

}
