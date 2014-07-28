package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceSnapshotterTckSpec extends SnapshotStoreSpec with EmbedMongo {

  lazy val config = ConfigFactory.parseString(s"""
      |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
      |akka.persistence.journal.leveldb.native = off
      |akka.contrib.persistence.mongodb.mongo.driver = "akka.contrib.persistence.mongodb.CasbahPersistenceExtension"
      |akka.contrib.persistence.mongodb.mongo.urls = ["localhost:$embedConnectionPort"]
      |akka-contrib-mongodb-persistence-snapshot {
	  |	  # Class name of the plugin.
	  |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
      |}
  """.stripMargin)

}