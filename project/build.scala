import sbt._
import sbt.Keys._
import Dependencies._
import com.twitter.sbt._
import com.typesafe.sbteclipse.plugin.EclipsePlugin._

object AppBuilder extends Build {
  
  val appSettings = Seq(
    name := "akka-persistence-mongo",
    organization := "com.github",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.10.2",
    EclipseKeys.withSource := true,
    EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
  )


  lazy val app = Project("akka-persistence-mongo", file("."))
    .settings(appSettings : _*)
    .settings(GitProject.gitSettings : _*)
    .settings(BuildProperties.newSettings : _*)
    .settings(PackageDist.newSettings : _*)
    .settings(ReleaseManagement.newSettings : _*)
    .settings(VersionManagement.newSettings : _*)
    .settings(libraryDependencies ++= appDependencies)
    .settings(resolvers ++= appResolvers)

}
