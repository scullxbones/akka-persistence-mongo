package akka.contrib.persistence.mongodb

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

object SnapshotTckSpec extends EmbeddedMongo {

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka.persistence.journal.leveldb.native = off
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://localhost:$embedConnectionPort"
    |akka-contrib-mongodb-persistence-snapshot {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    """.stripMargin)
}

abstract class SnapshotTckSpec(extensionClass: Class[_])
  extends SnapshotStoreSpec(SnapshotTckSpec.config(extensionClass)) with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    SnapshotTckSpec.doBefore()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SnapshotTckSpec.doAfter()
  }
}
