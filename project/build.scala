import sbt._
import sbt.Keys._
import Dependencies._

object AppBuilder extends Build {
  
  val VERSION = "0.0.1-SNAPSHOT"
  val SCALA_VERSION = "2.10.3"
  val ORG = "com.github.scullxbones"

  def projectSettings(moduleName: String) = 
    Seq(name := "akka-persistence-mongo-"+moduleName, 
        organization := ORG,
        version := VERSION,
        scalaVersion := SCALA_VERSION)
  
  val commonSettings = projectSettings("common")

  val casbahSettings = projectSettings("casbah")

  val rxmongoSettings = projectSettings("rxmongo")
  
  lazy val aRootNode = Project("root", file("."))
    			.settings(commonSettings : _*)
			.aggregate(casbah,rxmongo)

  lazy val common = Project("common", file("common"))
    .settings(commonSettings : _*)
    .settings(libraryDependencies ++= commonDependencies)
    .settings(resolvers ++= appResolvers)

  lazy val casbah = Project("casbah", file("casbah"))
    .settings(casbahSettings : _*)
    .settings(libraryDependencies ++= casbahDependencies)
    .settings(resolvers ++= appResolvers)
    .dependsOn(common % "test->test;compile->compile")

  lazy val rxmongo = Project("rxmongo", file("rxmongo"))
    .settings(rxmongoSettings : _*)
    .settings(libraryDependencies ++= rxmongoDependencies)
    .settings(resolvers ++= appResolvers)
    .dependsOn(common % "test->test;compile->compile")

}
