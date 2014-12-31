package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceSnapshotterTckSpec extends SnapshotTckSpec {
  def extensionClass = classOf[CasbahPersistenceExtension]
}