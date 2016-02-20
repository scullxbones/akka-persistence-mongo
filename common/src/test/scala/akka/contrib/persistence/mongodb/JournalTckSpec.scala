package akka.contrib.persistence.mongodb

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

object JournalTckSpec extends ContainerMongo {

  def config(extensionClass: Class[_], database: String) = ConfigFactory.parseString(s"""
     |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
     |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
     |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://$host:$noAuthPort"
     |akka.contrib.persistence.mongodb.mongo.database = $database
     |akka-contrib-mongodb-persistence-journal {
     |	  # Class name of the plugin.
     |  class = "akka.contrib.persistence.mongodb.MongoJournal"
     |}""".stripMargin)

}

abstract class JournalTckSpec(extensionClass: Class[_], dbName: String)
  extends JournalSpec(JournalTckSpec.config(extensionClass, dbName)) with BeforeAndAfterAll {

  override def supportsRejectingNonSerializableObjects = CapabilityFlag.off()

  override def afterAll() = {
    JournalTckSpec.cleanup(dbName)
    super.afterAll()
  }
}
