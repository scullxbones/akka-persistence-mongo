package akka.contrib.persistence.mongodb

import java.util

import com.mongodb.casbah.{MongoConnection, MongoDB}
import de.flapdoodle.embed.mongo._
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution._
import de.flapdoodle.embed.process.config.IRuntimeConfig
import de.flapdoodle.embed.process.distribution.Distribution
import de.flapdoodle.embed.process.extract._
import de.flapdoodle.embed.process.io.directories._
import de.flapdoodle.embed.process.runtime.{ICommandLinePostProcessor, Network}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._

trait Authentication {
  def injectCredentials(db: MongoDB, user: String = "admin", pass: String = "password"): Unit = db.addUser(user,pass)
}

class NoOpCommandLinePostProcessor extends ICommandLinePostProcessor with Authentication {
  override def process(distribution: Distribution, args: util.List[String]): util.List[String] = args
  override def injectCredentials(db: MongoDB, user: String = "admin", pass: String = "password") = ()
}

class AuthenticatingCommandLinePostProcessor(mechanism: String = "MONGODB-CR") extends ICommandLinePostProcessor with Authentication {
  override def process(distribution: Distribution, args: util.List[String]): util.List[String] =
    (args.asScala - "--noauth" :+ "--auth").asJava
}

trait EmbedMongo extends BeforeAndAfterAll { this: Suite =>
  def embedConnectionURL: String = { "localhost" }
  lazy val embedConnectionPort: Int = { Network.getFreeServerPort }
  def embedDB: String = "test"
  def auth: ICommandLinePostProcessor with Authentication = new NoOpCommandLinePostProcessor

  val artifactStorePath = new PlatformTempDir()
  val executableNaming = new UUIDTempNaming()
  val command = Command.MongoD
  val runtimeConfig: IRuntimeConfig  = new RuntimeConfigBuilder()
    .defaults(command)
    .artifactStore(new ArtifactStoreBuilder()
        .defaults(command)
        .download(new DownloadConfigBuilder()
            .defaultsForCommand(command)
            .artifactStorePath(artifactStorePath))
        .executableNaming(executableNaming))
        .commandLinePostProcessor(auth)
        .build()
  
  val mongodConfig = new MongodConfigBuilder()
    .version(Version.Main.V2_6)
    .cmdOptions(new MongoCmdOptionsBuilder()
    	.syncDelay(1)
      .useNoJournal(false)
      .useNoPrealloc(true)
      .useSmallFiles(true)
      .verbose(false)
    	.build())
    .net(new Net("127.0.0.1",embedConnectionPort, Network.localhostIsIPv6()))
    .build()

  lazy val runtime = MongodStarter.getInstance(runtimeConfig)
  lazy val mongod = runtime.prepare(mongodConfig)
  lazy val mongodExe = mongod.start()

  override def beforeAll() {
    mongodExe
    auth.injectCredentials(mongoDB)
    super.beforeAll()
  }

  override def afterAll() {
    super.afterAll()
    mongod.stop(); mongodExe.stop()
  }

  lazy val mongoDB = MongoConnection(embedConnectionURL, embedConnectionPort)(embedDB)
}