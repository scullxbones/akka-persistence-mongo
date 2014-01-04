import sbt._

object Dependencies {

  val typesafeReleases = "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
  val typesafeSnapshots = "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
  val local = "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

  val appResolvers = Seq(
    typesafeReleases,
    typesafeSnapshots,
    Resolver.sonatypeRepo("snapshots")
  )

  val scalatest = "org.scalatest" %% "scalatest" % "2.0" % "test"
  val junit = "junit" % "junit" % "4.11" % "test"
  val scalaMock = "org.scalamock" %% "scalamock-scalatest-support" % "3.1.RC1" % "test"
  val embedMongo = "com.github.simplyscala" %% "scalatest-embedmongo" % "0.2.1" % "test"

  val v = Map(
    'akka -> "2.3-M2",
    'casbah -> "2.6.4",
    'rxmongo -> "0.10.0"
  )

  val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-experimental" % v('akka)
  val rxMongo = "org.reactivemongo" %% "reactivemongo" % v('rxmongo)
  val casbah = "org.mongodb" %% "casbah" % v('casbah)
 
  val testDependencies = Seq(
    scalatest,
    junit,
    scalaMock
  )
 
  val commonDependencies = Seq(
    akkaPersistence
  ) ++ testDependencies

  val casbahDependencies = Seq(
    casbah,
    embedMongo 
  ) ++ commonDependencies

  val rxmongoDependencies = Seq(
    rxMongo,
    embedMongo 
  ) ++ commonDependencies

}
