import sbt._

object Dependencies {

  val AKKA_VERSION = "2.3.8"

  val typesafeReleases = "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
  val typesafeSnapshots = "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
  val local = "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

  val projectResolvers = Seq(
    typesafeReleases,
    typesafeSnapshots,
    Resolver.sonatypeRepo("snapshots")
  )

  val scalatest = "org.scalatest" %% "scalatest" % "2.1.7" % "test"
  val junit = "junit" % "junit" % "4.11" % "test"
  val scalaMock = "org.scalamock" %% "scalamock-scalatest-support" % "3.1.RC1" % "test"
  val mockito = "org.mockito" % "mockito-all" % "1.9.5" % "test"
  val embedMongoScalatest = "com.github.simplyscala" %% "scalatest-embedmongo" % "0.2.1" % "test"
  val embedMongo = "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.46.4" % "test"
  val mongoDriver = "org.mongodb" % "mongo-java-driver" % "2.12.4" % "test"
  val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION % "test"
  val akkaPersistTck = "com.typesafe.akka" %% "akka-persistence-tck-experimental" % AKKA_VERSION % "test"

  val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-experimental" % AKKA_VERSION % "provided"
  val rxMongo = "org.reactivemongo" %% "reactivemongo" % "0.10.5.0.akka23" % "provided"
  val casbah = "org.mongodb" %% "casbah" % "2.7.4" % "provided"
  val metrix = "nl.grons" %% "metrics-scala" % "3.3.0_a2.3"
 
  val testDependencies = Seq(
    scalatest,
    junit,
    mockito,
    akkaTestKit,
    akkaPersistTck,
    embedMongo,
    mongoDriver
  )
 
  val commonDependencies = Seq(
    akkaPersistence,
    metrix
  ) ++ testDependencies

  val casbahDependencies = Seq(
    casbah
  ) ++ commonDependencies

  val rxmongoDependencies = Seq(
    rxMongo
  ) ++ commonDependencies

}
