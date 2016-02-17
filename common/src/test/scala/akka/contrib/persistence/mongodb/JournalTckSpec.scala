package akka.contrib.persistence.mongodb

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

object JournalTckSpec extends EmbeddedMongo {

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
     |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
     |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
     |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://localhost:$embedConnectionPort"
     |akka-contrib-mongodb-persistence-journal {
     |	  # Class name of the plugin.
     |  class = "akka.contrib.persistence.mongodb.MongoJournal"
     |}""".stripMargin)

}

abstract class JournalTckSpec(extensionClass: Class[_])
  extends JournalSpec(JournalTckSpec.config(extensionClass)) with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    JournalTckSpec.doBefore()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    JournalTckSpec.doAfter()
  }

  override def supportsRejectingNonSerializableObjects = CapabilityFlag.off()

}
