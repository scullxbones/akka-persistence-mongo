import sbt._
import sbt.Keys._
import Dependencies._
import xerial.sbt.Sonatype._
import SonatypeKeys._

object AppBuilder extends Build {
  
  val VERSION = "0.1.0"
  val SCALA_VERSION = "2.10.0"
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

  def project(moduleName: String) = 
    Project(moduleName, file(moduleName))
	.settings(projectSettings(moduleName) : _*)
	.settings(resolvers ++= projectResolvers)

  def projectSettings(moduleName: String) = Defaults.defaultSettings ++
    Seq(name := "akka-persistence-mongo-"+moduleName, 
        organization := ORG,
        version := VERSION,
        scalaVersion := SCALA_VERSION,
        crossScalaVersions := Seq("2.10.0","2.11.0"),
        pomExtra := POM_XTRA,
	scalacOptions ++= Seq("-unchecked", "-deprecation","-feature")) ++ sonatypeSettings
  
  lazy val aRootNode = Project("root", file("."))
			.settings (packagedArtifacts in file(".") := Map.empty)
			.settings (crossScalaVersions := Seq("2.10.0","2.11.0"))
			.settings (scalaVersion := SCALA_VERSION)
			.settings (publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))))
			.aggregate(common,casbah)

  lazy val common = project("common")
    .settings(libraryDependencies ++= commonDependencies)

  lazy val casbah = project("casbah")
    .settings(libraryDependencies ++= casbahDependencies)
    .dependsOn(common % "test->test;compile->compile")
/*
  lazy val rxmongo = project("rxmongo")
    .settings(libraryDependencies ++= rxmongoDependencies)
    .dependsOn(common % "test->test;compile->compile")
*/

}
