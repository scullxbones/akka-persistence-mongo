package akka.contrib.persistence.mongodb

import com.mongodb._
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution.{IFeatureAwareVersion, Version}
import de.flapdoodle.embed.mongo.{Command, MongodStarter}
import de.flapdoodle.embed.process.config.IRuntimeConfig
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.process.extract.{NoopTempNaming, UUIDTempNaming, UserTempNaming}
import de.flapdoodle.embed.process.io.directories.{TempDirInPlatformTempDir, UserHome}
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

  def injectCredentials(db: DB, user: String = "admin", pass: String = "password"): Unit = {
    val command = Map("createUser" -> user,
                      "pwd" -> pass,
                      "roles" -> List("userAdminAnyDatabase","dbAdminAnyDatabase","readWrite"))
    val result = db.command(command.asDBObject)
    if (!result.ok()) {
      result.keySet().asScala.foreach(k => println(s"k-v: $k = ${result.get(k)}"))
      println(s"${result.getErrorMessage}")
      println(s"${result.getException}")
      println(s"${command.asDBObject}")
      result.throwOnError()
    }
  }
}

class NoOpCommandLinePostProcessor extends (MongoCmdOptionsBuilder => MongoCmdOptionsBuilder) with Authentication {
  override def apply(builder: MongoCmdOptionsBuilder): MongoCmdOptionsBuilder = builder.enableAuth(false)
  override def injectCredentials(db: DB, user: String = "admin", pass: String = "password") = ()
}

class AuthenticatingCommandLinePostProcessor(mechanism: String = "MONGODB-CR") extends (MongoCmdOptionsBuilder => MongoCmdOptionsBuilder) with Authentication {
  override def apply(builder: MongoCmdOptionsBuilder): MongoCmdOptionsBuilder = builder.enableAuth(true)
}

object EmbeddedMongo {
  val command = Command.MongoD
  val runtimeConfig: IRuntimeConfig  = new RuntimeConfigBuilder()
    .defaults(command)
    .artifactStore(new ExtractedArtifactStoreBuilder()
      .defaults(command)
      .download(new DownloadConfigBuilder()
        .defaultsForCommand(command)
        .artifactStorePath(new UserHome(".embedmongo/artifacts"))
        .build()
      )
      .tempDir(new TempDirInPlatformTempDir)
      .extractExecutableNaming(new NoopTempNaming)
      .executableNaming(new NoopTempNaming)
      .build()
    )
    .processOutput(ProcessOutput.getDefaultInstanceSilent)
    .build()
  val starter = MongodStarter.getInstance(runtimeConfig)
}

trait EmbeddedMongo {
  def embedConnectionURL: String = { "localhost" }
  lazy val embedConnectionPort: Int = { Network.getFreeServerPort }
  def embedDB: String = "test"
  def auth: (MongoCmdOptionsBuilder => MongoCmdOptionsBuilder) with Authentication = new NoOpCommandLinePostProcessor

  def envMongoVersion = Option(System.getenv("MONGODB_VERSION")).orElse(Option("3.0"))
  def overrideOptions: MongoCmdOptionsBuilder => MongoCmdOptionsBuilder = auth andThen useWiredTigerOn30

  def useWiredTigerOn30(builder: MongoCmdOptionsBuilder): MongoCmdOptionsBuilder =
    envMongoVersion.filter(_ == "3.0").map(_ => builder.useStorageEngine("wiredTiger")).getOrElse(builder)

  def determineVersion: IFeatureAwareVersion =
    envMongoVersion.collect {
      case "2.4" => Version.Main.V2_4
      case "2.6" => Version.Main.V2_6
      case "3.0" => Version.Main.V3_0
    }.getOrElse(Version.Main.PRODUCTION)

  def mongodConfig = new MongodConfigBuilder()
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

  lazy val mongod = EmbeddedMongo.starter.prepare(mongodConfig)
  lazy val mongodExe = mongod.start()

  def mongoClient = new MongoClient(embedConnectionURL,embedConnectionPort)

  def doBefore(): Unit = {
    mongodExe
    auth.injectCredentials(mongoClient.getDB("admin"))
  }

  def doAfter(): Unit = {
    mongod.stop()
    if (mongodExe.isProcessRunning) mongodExe.stop()
  }
}
