package akka.contrib.persistence.mongodb

import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext

trait MongoPersistenceSpec[D,C] extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll { self: TestKit =>

  implicit val callerRuns = new ExecutionContext {
    def reportFailure(t: Throwable): Unit = { t.printStackTrace() }
    def execute(runnable: Runnable): Unit = { runnable.run() }
  }

  def driver:D

  def withCollection(name: String)(testCode: C => Any):Any

  def withJournal(testCode: C => Any):Any

  def withSnapshot(testCode: C => Any):Any

  override def beforeAll(): Unit = {
    doBefore()
  }

  override def afterAll(): Unit = {
    doAfter()
  }
}
