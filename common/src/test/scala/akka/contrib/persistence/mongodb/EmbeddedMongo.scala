package akka.contrib.persistence.mongodb

import java.util

import com.mongodb.client.MongoDatabase
import de.flapdoodle.embed.mongo.{MongodStarter, Command}
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.config.IRuntimeConfig
import de.flapdoodle.embed.process.distribution.Distribution
import de.flapdoodle.embed.process.extract.UUIDTempNaming
import de.flapdoodle.embed.process.io.directories.PlatformTempDir
import de.flapdoodle.embed.process.runtime.{Network, ICommandLinePostProcessor}
import scala.collection.JavaConverters._
import com.mongodb._

trait Authentication {
  implicit class MongoFiedMap(map: Map[String,Any]) {
    def asDBObject: BasicDBObject = {
      map.foldLeft(BasicDBObjectBuilder.start()) { case(builder,(k,v)) => v match {
        case v:Map[String,Any] => builder.add(k,v.asDBObject)
        case v:List[Any] => builder.add(k,v.asJava)
        case _ => builder.add(k,v)
      }}.get().asInstanceOf[BasicDBObject]
    }
  }

  def injectCredentials(db: MongoDatabase, user: String = "admin", pass: String = "password"): Unit = {
    val roles = BasicDBObjectBuilder.start("role","userAdminAnyDatabase").add("db","admin").get ::
                BasicDBObjectBuilder.start("role","dbAdminAnyDatabase").add("db","admin").get ::
                BasicDBObjectBuilder.start("role","readWrite").add("db","admin").get ::
//                BasicDBObjectBuilder.start("role","root").add("db","admin").get ::
                Nil
    val command = Map("createUser" -> user,
                      "pwd" -> pass,
                      "roles" -> List("userAdminAnyDatabase","dbAdminAnyDatabase","readWrite"))
    val result = db.runCommand(command.asDBObject)
    if (result.getInteger("ok") != 1) {
      result.keySet().asScala.foreach(k => println(s"k-v: $k = ${result.get(k)}"))
      println(s"${command.asDBObject}")
      throw new Exception("Could not successfully create user")
    }
  }
}

class NoOpCommandLinePostProcessor extends ICommandLinePostProcessor with Authentication {
  override def process(distribution: Distribution, args: util.List[String]): util.List[String] = args
  override def injectCredentials(db: MongoDatabase, user: String = "admin", pass: String = "password") = ()
}

class AuthenticatingCommandLinePostProcessor(mechanism: String = "MONGODB-CR") extends ICommandLinePostProcessor with Authentication {
  override def process(distribution: Distribution, args: util.List[String]): util.List[String] =
    (args.asScala - "--noauth" :+ "--auth").asJava
}

trait EmbeddedMongo {
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
    .version(Version.Main.V3_0)
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

  lazy val mongoClient = new MongoClient(embedConnectionURL,embedConnectionPort)

  def doBefore(): Unit = {
    mongodExe
    auth.injectCredentials(mongoClient.getDatabase("admin"))
  }

  def doAfter(): Unit = {
    mongod.stop(); mongodExe.stop()
  }
}
