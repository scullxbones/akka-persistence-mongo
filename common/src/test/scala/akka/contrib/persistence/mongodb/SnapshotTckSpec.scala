package akka.contrib.persistence.mongodb

import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

abstract class SnapshotTckSpec extends SnapshotStoreSpec with EmbeddedMongo {

  def extensionClass: Class[_]

  lazy val config = ConfigFactory.parseString(s"""
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka.persistence.journal.leveldb.native = off
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.urls = ["localhost:$embedConnectionPort"]
    |akka-contrib-mongodb-persistence-snapshot {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    """.stripMargin)

}
