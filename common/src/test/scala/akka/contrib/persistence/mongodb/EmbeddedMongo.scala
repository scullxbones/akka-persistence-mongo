package akka.contrib.persistence.mongodb

import com.mongodb._
import com.mongodb.client.MongoDatabase
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution.{IFeatureAwareVersion, Version}
import de.flapdoodle.embed.mongo.{Command, MongodStarter}
import de.flapdoodle.embed.process.config.IRuntimeConfig
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.process.extract.UUIDTempNaming
import de.flapdoodle.embed.process.io.directories.PlatformTempDir
import de.flapdoodle.embed.process.runtime.Network

import scala.collection.JavaConverters._

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
    val command = Map("createUser" -> user,
                      "pwd" -> pass,
                      "roles" -> List("userAdminAnyDatabase","dbAdminAnyDatabase","readWrite"))
    val result = db.runCommand(command.asDBObject)
    if (result.get("ok").toString != "1.0") {
      result.keySet().asScala.foreach(k => println(s"k-v: $k = ${result.get(k)}"))
//      println(s"${result.getErrorMessage}")
//      println(s"${result.getException}")
      println(s"${command.asDBObject}")
      throw new Exception(s"Failed to inject credentials - error message = ${result.get("errmsg")}")
    }
  }
}

class NoOpCommandLinePostProcessor extends (MongoCmdOptionsBuilder => MongoCmdOptionsBuilder) with Authentication {
  override def apply(builder: MongoCmdOptionsBuilder): MongoCmdOptionsBuilder = builder.enableAuth(false)
  override def injectCredentials(db: MongoDatabase, user: String = "admin", pass: String = "password") = ()
}

class AuthenticatingCommandLinePostProcessor(mechanism: String = "MONGODB-CR") extends (MongoCmdOptionsBuilder => MongoCmdOptionsBuilder) with Authentication {
  override def apply(builder: MongoCmdOptionsBuilder): MongoCmdOptionsBuilder = builder.enableAuth(true)
}

trait EmbeddedMongo {
  def embedConnectionURL: String = { "localhost" }
  lazy val embedConnectionPort: Int = { Network.getFreeServerPort }
  def embedDB: String = "test"
  def auth: (MongoCmdOptionsBuilder => MongoCmdOptionsBuilder) with Authentication = new NoOpCommandLinePostProcessor

  def envMongoVersion = Option(System.getenv("MONGODB_VERSION")).orElse(Option("3.2"))
  def overrideOptions: MongoCmdOptionsBuilder => MongoCmdOptionsBuilder = auth andThen useWiredTigerOn30

  def useWiredTigerOn30(builder: MongoCmdOptionsBuilder): MongoCmdOptionsBuilder =
    envMongoVersion.filter("3.0" :: "3.2" :: Nil contains(_)).map(_ => builder.useStorageEngine("wiredTiger")).getOrElse(builder)

  def determineVersion: IFeatureAwareVersion =
    envMongoVersion.collect {
      case "2.4" => Version.Main.V2_4
      case "2.6" => Version.Main.V2_6
      case "3.0" => Version.Main.V3_0
      case "3.2" => Version.Main.V3_2
    }.getOrElse(Version.Main.PRODUCTION)

  val artifactStorePath = new PlatformTempDir()
  val executableNaming = new UUIDTempNaming()
  val command = Command.MongoD
  val runtimeConfig: IRuntimeConfig  = new RuntimeConfigBuilder()
    .defaults(command)
    .artifactStore(new ArtifactStoreBuilder()
                      .defaults(command)
                      .download(new DownloadConfigBuilder()
                                    .defaultsForCommand(command)
                                    .artifactStorePath(artifactStorePath)
                      ).executableNaming(executableNaming)
    ).processOutput(ProcessOutput.getDefaultInstanceSilent)
    .build()

  val mongodConfig = new MongodConfigBuilder()
    .version(determineVersion)
    .cmdOptions(
      overrideOptions(new MongoCmdOptionsBuilder()
                          .syncDelay(1)
                          .useNoJournal(false)
                          .useNoPrealloc(true)
                          .useSmallFiles(true)
                          .verbose(false)
      ).build()
    )
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
    mongoClient.close()
    mongod.stop()
    if (mongodExe.isProcessRunning) mongodExe.stop()
  }
}

trait ContainerMongo {
  def host = sys.env.getOrElse("DOCKER_HOST","localhost")
  def authPort = 28117
  def noAuthPort = 27117
}