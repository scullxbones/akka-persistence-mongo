import sbt._
import sbt.Keys._
import Dependencies._
import xerial.sbt.Sonatype._
import SonatypeKeys._

object AppBuilder extends Build {
  
  val VERSION = "0.0.8-SNAPSHOT"
  val SCALA_VERSION = "2.10.3"
  val ORG = "com.github.scullxbones"
  val POM_XTRA = {
  <url>https://github.com/scullxbones/akka-persistence-mongo</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:github.com/scullxbones/akka-persistence-mongo.git</connection>
    <developerConnection>scm:git:git@github.com:scullxbones/akka-persistence-mongo.git</developerConnection>
    <url>github.com/scullxbones/akka-persistence-mongo.git</url>
  </scm>
  <developers>
    <developer>
      <id>scullxbones</id>
      <name>Brian Scully</name>
      <url>https://github.com/scullxbones/</url>
    </developer>
  </developers>
  }

  def projectSettings(moduleName: String) = 
    Seq(name := "akka-persistence-mongo-"+moduleName, 
        organization := ORG,
        version := VERSION,
        scalaVersion := SCALA_VERSION,
        pomExtra := POM_XTRA,
	scalacOptions ++= Seq("-unchecked", "-deprecation","-feature")) ++ sonatypeSettings
  
  val commonSettings = projectSettings("common")

  val casbahSettings = projectSettings("casbah")

  val rxmongoSettings = projectSettings("rxmongo")
  
  lazy val aRootNode = Project("root", file("."))
			.settings (packagedArtifacts in file(".") := Map.empty)
			.aggregate(common,casbah,rxmongo)

  lazy val common = Project("common", file("common"))
    .settings(commonSettings : _*)
    .settings(libraryDependencies ++= commonDependencies)
    .settings(resolvers ++= projectResolvers)

  lazy val casbah = Project("casbah", file("casbah"))
    .settings(casbahSettings : _*)
    .settings(libraryDependencies ++= casbahDependencies)
    .settings(resolvers ++= projectResolvers)
    .dependsOn(common % "test->test;compile->compile")

  lazy val rxmongo = Project("rxmongo", file("rxmongo"))
    .settings(rxmongoSettings : _*)
    .settings(libraryDependencies ++= rxmongoDependencies)
    .settings(resolvers ++= projectResolvers)
    .dependsOn(common % "test->test;compile->compile")


}
