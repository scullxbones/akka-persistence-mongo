package akka.contrib.persistence.mongodb

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

abstract class JournalTckSpec extends JournalSpec with EmbeddedMongo {

    def extensionClass: Class[_]

    lazy val config = ConfigFactory.parseString(s"""
      |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
      |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
      |akka.contrib.persistence.mongodb.mongo.urls = ["localhost:$embedConnectionPort"]
      |akka-contrib-mongodb-persistence-journal {
      |	  # Class name of the plugin.
      |  class = "akka.contrib.persistence.mongodb.MongoJournal"
      |}
    """.stripMargin)

}
