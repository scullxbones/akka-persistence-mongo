/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext

trait MongoPersistenceSpec[D,C] extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll { self: TestKit =>

  implicit val callerRuns = new ExecutionContext {
    def reportFailure(t: Throwable): Unit = { throw t }
    def execute(runnable: Runnable): Unit = { runnable.run() }
  }

  override def beforeAll() = cleanup()

  def driver:D
  
  def extendedDriver:D

  def withCollection(name: String)(testCode: C => Any):Any
  
  def withJournalCollections(testCode: D => Any):Any
  
  def withSnapshotCollections(testCode: D => Any):Any

  def withJournal(testCode: C => Any):Any
  
  def withSuffixedJournal(pid: String)(testCode: C => Any):Any
  
  def withAutoSuffixedJournal(testCode: D => Any):Any

  def withSnapshot(testCode: C => Any):Any
  
  def withSuffixedSnapshot(pid: String)(testCode: C => Any):Any
  
  def withAutoSuffixedSnapshot[T](testCode: D => T): Unit
}
