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

  val scalatest = "org.scalatest" %% "scalatest" % "1.9.2"
  val junit = "junit" % "junit" % "4.11"
  val embedMongo = "com.github.simplyscala" %% "scalatest-embedmongo" % "0.2.1"

  val v = Map(
    'akka -> "2.3-M2",
    'casbah -> "2.6.4",
    'rxmongo -> "0.10.0"
  )

  val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-experimental" % v('akka)
  val rxMongo = "org.reactivemongo" %% "reactivemongo" % v('rxmongo)
  val casbah = "org.mongodb" %% "casbah" % v('casbah)

  val commonDependencies = Seq(
    akkaPersistence,
    scalatest % "test",
    junit % "test",
    embedMongo % "test"
  )

  val casbahDependencies = Seq(
    casbah
  )

  val rxmongoDependencies = Seq(
    rxMongo
  )

}
