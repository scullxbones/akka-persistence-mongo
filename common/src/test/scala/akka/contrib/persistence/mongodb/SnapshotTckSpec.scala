package akka.contrib.persistence.mongodb

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

object SnapshotTckSpec extends ContainerMongo {

  def config(extensionClass: Class[_], database: String) = ConfigFactory.parseString(s"""
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka.persistence.journal.leveldb.native = off
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://$host:$noAuthPort"
    |akka.contrib.persistence.mongodb.mongo.database = $database
    |akka-contrib-mongodb-persistence-snapshot {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    """.stripMargin)
}

abstract class SnapshotTckSpec(extensionClass: Class[_], dbName: String)
  extends SnapshotStoreSpec(SnapshotTckSpec.config(extensionClass,dbName)) with BeforeAndAfterAll {

  override def afterAll() = {
    SnapshotTckSpec.cleanup(dbName)
    super.afterAll()
  }

}
