package akka.contrib.persistence.mongodb

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.MongodExecutable
import de.flapdoodle.embed.mongo.MongodProcess
import de.flapdoodle.embed.mongo.config.MongodConfig
import com.mongodb.casbah.MongoConnection

trait EmbedMongo extends BeforeAndAfterAll { this: BeforeAndAfterAll with Suite =>
  def embedConnectionURL: String = { "localhost" }
  def embedConnectionPort: Int = { 12345 }
  def embedMongoDBVersion: Version = { Version.V2_2_1 }
  def embedDB: String = { "test" }

  lazy val runtime: MongodStarter = MongodStarter.getDefaultInstance
  lazy val mongodExe: MongodExecutable = runtime.prepare(new MongodConfig(embedMongoDBVersion, embedConnectionPort, true))
  lazy val mongod: MongodProcess = mongodExe.start()

  override def beforeAll() {
    mongod
    super.beforeAll()
  }

  override def afterAll() {
    super.afterAll()
    mongod.stop(); mongodExe.stop()
  }

  lazy val mongoDB = MongoConnection(embedConnectionURL, embedConnectionPort)(embedDB)
}