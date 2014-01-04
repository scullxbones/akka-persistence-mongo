import sbt._
import sbt.Keys._
import Dependencies._

object AppBuilder extends Build {
  
  val commonSettings = Seq(
    name := "akka-persistence-mongo-common",
    organization := "com.github.scullxbones",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.10.2"
  )

  val casbahSettings = Seq(
    name := "akka-persistence-mongo-casbah",
    organization := "com.github.scullxbones",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.10.2"
  )

  val rxmongoSettings = Seq(
    name := "akka-persistence-mongo-rxmongo",
    organization := "com.github.scullxbones",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.10.2"
  )

  lazy val common = Project("common", file("common"))
    .settings(commonSettings : _*)
    .settings(libraryDependencies ++= commonDependencies)
    .settings(resolvers ++= appResolvers)

  lazy val casbah = Project("casbah", file("casbah"))
    .settings(casbahSettings : _*)
    .settings(libraryDependencies ++= casbahDependencies)
    .settings(resolvers ++= appResolvers)
    .dependsOn(common)
    .aggregate(common)

  lazy val rxmongo = Project("rxmongo", file("rxmongo"))
    .settings(rxmongoSettings : _*)
    .settings(libraryDependencies ++= rxmongoDependencies)
    .settings(resolvers ++= appResolvers)
    .dependsOn(common)
    .aggregate(common)

}
