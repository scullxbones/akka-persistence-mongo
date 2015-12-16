import sbt.Credentials
import sbt.Keys._

val releaseV = "1.1.2-di2"

val scalaV = "2.11.7"

scalaVersion := scalaV

val AkkaV = "2.4.0"

val pomXtra = {
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

val commonDeps = Seq(
  ("com.typesafe.akka" %% "akka-persistence" % AkkaV % "provided")
    .exclude("org.iq80.leveldb", "leveldb")
    .exclude("org.fusesource.leveldbjni", "leveldbjni-all"),
  ("nl.grons" %% "metrics-scala" % "3.5.1_a2.3")
    .exclude("com.typesafe.akka", "akka-actor_2.10")
    .exclude("com.typesafe.akka", "akka-actor_2.11"),
  "com.typesafe.akka" %% "akka-persistence-query-experimental" % AkkaV % "provided",
  "org.mongodb" % "mongo-java-driver" % "2.13.1" % "test",
  "org.slf4j" % "slf4j-simple" % "1.7.12" % "test",
  "org.scalatest" %% "scalatest" % "2.1.7" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.50.1" % "test",
  "com.typesafe.akka" %% "akka-testkit" % AkkaV % "test",
  "com.typesafe.akka" %% "akka-persistence-tck" % AkkaV % "test",
  "com.typesafe.akka" %% "akka-cluster-sharding" % AkkaV % "test"
)

val commonSettings = Seq(
  scalaVersion := scalaV,
  libraryDependencies ++= commonDeps,
  version := releaseV,
  organization := "com.github.scullxbones",
  pomExtra := pomXtra,
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-encoding", "UTF-8",       // yes, this is 2 args
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    // "-Xfatal-warnings",      Deprecations keep from enabling this
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",        // N.B. doesn't work well with the ??? hole
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture",
    "-Ywarn-unused-import",     // 2.11 only
    "-target:jvm-1.8"
  ),
  javacOptions ++= Seq(
    "-source", "1.8",
    "-target", "1.8",
    "-Xlint"
  ),
  resolvers ++= Seq(
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
    "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  ),
  parallelExecution in Test := false,
)

lazy val `akka-persistence-mongo-common` = (project in file("common"))
  .settings(commonSettings:_*)

lazy val `akka-persistence-mongo-casbah` = (project in file("casbah"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb" %% "casbah" % "2.8.1" % "provided"
    )
  )

lazy val `akka-persistence-mongo-rxmongo` = (project in file("rxmongo"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      ("org.reactivemongo" %% "reactivemongo" % "0.11.6" % "provided")
        .exclude("com.typesafe.akka","akka-actor_2.10")
        .exclude("com.typesafe.akka","akka-actor_2.11")
    )
  )

lazy val root = (project in file("."))
  .aggregate(`akka-persistence-mongo-common`, `akka-persistence-mongo-casbah`, `akka-persistence-mongo-rxmongo`)
  .settings(commonSettings:_*)
  .settings(
    packagedArtifacts in file(".") := Map.empty
  )
