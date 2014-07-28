package akka.contrib.persistence.mongodb

import com.typesafe.config.ConfigFactory
import akka.persistence.journal.JournalSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CasbahPersistenceJournalTckSpec extends JournalSpec with EmbedMongo {
  
  lazy val config = ConfigFactory.parseString(s"""
      |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
      |akka.contrib.persistence.mongodb.mongo.driver = "akka.contrib.persistence.mongodb.CasbahPersistenceExtension"
      |akka.contrib.persistence.mongodb.mongo.urls = ["localhost:$embedConnectionPort"]
      |akka-contrib-mongodb-persistence-journal {
	  |	  # Class name of the plugin.
	  |  class = "akka.contrib.persistence.mongodb.MongoJournal"
      |}
  """.stripMargin)

}