/* 
 * Contributions:
 * Jean-Francois GUENA: implement "suffixed collection name" feature (issue #39 partially fulfilled)
 * ...
 */

package akka.contrib.persistence.mongodb

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

object JournalTckSpec extends ContainerMongo {

  def config(extensionClass: Class[_], database: String, extendedConfig: String = "|") =
    ConfigFactory.parseString(s"""
     |include "/application.conf"
     |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
     |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
     |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://$host:$noAuthPort"
     |akka.contrib.persistence.mongodb.mongo.database = $database
     |akka-contrib-mongodb-persistence-journal {
     |	  # Class name of the plugin.
     |  class = "akka.contrib.persistence.mongodb.MongoJournal"
     |}
     $extendedConfig
     |""".stripMargin).withFallback(ConfigFactory.defaultReference()).resolve()

}

abstract class JournalTckSpec(extensionClass: Class[_], dbName: String, extendedConfig: String = "|")
  extends JournalSpec(JournalTckSpec.config(extensionClass, dbName, extendedConfig)) with BeforeAndAfterAll {

  override def supportsRejectingNonSerializableObjects = CapabilityFlag.on()

  override def afterAll() = {
    super.afterAll()
    JournalTckSpec.cleanup(dbName)
  }
}
