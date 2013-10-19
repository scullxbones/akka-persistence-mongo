import sbt._

object Dependencies {

  val typesafeReleases = "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
  val typesafeSnapshots = "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"

  val appResolvers = Seq(
    typesafeReleases,
    typesafeSnapshots,
    Resolver.sonatypeRepo("snapshots")
  )

  val scalatest = "org.scalatest" %% "scalatest" % "1.9.2"
  val junit = "junit" % "junit" % "4.11"

  val v = Map(
    'akka -> "2.3-M1",
    'mongo -> "0.9",
    'pickling -> "0.8.0-SNAPSHOT"
  )

  val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-experimental" % v('akka)
  val akkaDurableMailboxen = "com.typesafe.akka" %% "akka-mailboxes-common" % v('akka)
  val rxMongo = "org.reactivemongo" %% "reactivemongo" % v('mongo)
  val pickling = "org.scala-lang" %% "scala-pickling" % v('pickling)

  val appDependencies = Seq(
    akkaPersistence,
    akkaDurableMailboxen,
    rxMongo,
    pickling,
    scalatest % "test",
    junit % "test"
  )

}
