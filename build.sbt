publish / skip := true

val scala212V = "2.12.15"
val scala213V = "2.13.12"

val scalaV = scala213V
val akkaV = "2.6.20"

val MongoJavaDriverVersion = "4.10.2"
val Log4JVersion = "2.17.0"
val NettyVersion = "4.1.98.Final"

val commonDeps = Seq(
  ("com.typesafe.akka"  %% "akka-persistence" % akkaV)
    .exclude("org.iq80.leveldb", "leveldb")
    .exclude("org.fusesource.leveldbjni", "leveldbjni-all"),
  ("nl.grons" %% "metrics4-akka_a25" % "4.2.9")
    .exclude("com.typesafe.akka", "akka-actor_2.11")
    .exclude("com.typesafe.akka", "akka-actor_2.12")
    .exclude("com.typesafe.akka", "akka-actor_2.13"),
  "com.typesafe.akka"         %% "akka-persistence-query"   % akkaV     % "compile",
  "com.typesafe.akka"         %% "akka-persistence"         % akkaV     % "compile",
  "com.typesafe.akka"         %% "akka-actor"               % akkaV     % "compile",
  "org.mongodb"               % "mongodb-driver-core"       % MongoJavaDriverVersion   % "compile",
  "org.mongodb"               % "mongodb-driver-legacy"     % MongoJavaDriverVersion   % "test",
  "org.slf4j"                 % "slf4j-api"                 % "1.7.36"  % "test",
  "org.apache.logging.log4j"  % "log4j-api"                 % Log4JVersion  % "test",
  "org.apache.logging.log4j"  % "log4j-core"                % Log4JVersion  % "test",
  "org.apache.logging.log4j"  % "log4j-slf4j-impl"          % Log4JVersion  % "test",
  "org.scalatest"             %% "scalatest"                % "3.2.17"   % "test",
  "org.scalatestplus"         %% "mockito-1-10"             % "3.1.0.0" % "test",
  "org.scalatestplus"         %% "junit-4-13"               % "3.2.17.0" % "test",
  "junit"                     % "junit"                     % "4.13.2"    % "test",
  "org.mockito"               % "mockito-all"               % "1.10.19" % "test",
  "com.typesafe.akka"         %% "akka-slf4j"               % akkaV     % "test",
  "com.typesafe.akka"         %% "akka-testkit"             % akkaV     % "test",
  "com.typesafe.akka"         %% "akka-persistence-tck"     % akkaV     % "test",
  "com.typesafe.akka"         %% "akka-cluster-sharding"    % akkaV     % "test"
)

lazy val Ci = config("ci").extend(Test)

ThisBuild / organization := "com.github.scullxbones"
ThisBuild / scalaVersion := scalaV
ThisBuild / versionScheme := Some("semver-spec")

import xerial.sbt.Sonatype._

ThisBuild / sonatypeProjectHosting := Some(GitHubHosting("scullxbones", "akka-persistence-mongo", "scullduggery@gmail.com"))
ThisBuild / licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / developers := List(
    Developer(
      "scullxbones",
      "Brian Scully",
      "@scullxbones",
      url("https://github.com/scullxbones/")
    )
  )
ThisBuild / homepage := Some(url("https://github.com/scullxbones/akka-persistence-mongo"))

val commonSettings = Seq(
  scalaVersion := scalaV,
  crossScalaVersions := Seq(scala212V, scala213V),
  libraryDependencies ++= commonDeps,
  dependencyOverrides ++= Seq(
    "com.typesafe" % "config" % "1.3.2",
    "org.slf4j" % "slf4j-api" % "1.7.36",
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "org.mongodb" % "mongodb-driver-legacy" % MongoJavaDriverVersion
  ),
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
    "-Ywarn-dead-code",        // N.B. doesn't work well with the ??? hole
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture",
    "-target:jvm-1.8"
  ),
  javacOptions ++= Seq(
    "-source", "1.8",
    "-target", "1.8",
    "-Xlint"
  ),
  resolvers ++= Seq(
    "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/",
    "Typesafe Snapshots" at "https://repo.typesafe.com/typesafe/snapshots/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  ),
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oDS"),
  Ci / testOptions += Tests.Argument("-l", "org.scalatest.tags.Slow"),
  Test / fork := false,
) ++ inConfig(Ci)(Defaults.testTasks)

lazy val `akka-persistence-mongo-common` = (project in file("common"))
  .settings(commonSettings:_*)
  .configs(Ci)

lazy val `akka-persistence-mongo-scala` = (project in file("scala"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % MongoJavaDriverVersion % "compile",
      "org.mongodb.scala" %% "mongo-scala-bson"   % MongoJavaDriverVersion % "compile",
      "io.netty"          % "netty-buffer"        % NettyVersion % "compile",
      "io.netty"          % "netty-transport"     % NettyVersion % "compile",
      "io.netty"          % "netty-handler"       % NettyVersion % "compile",
      "org.reactivestreams" % "reactive-streams"  % "1.0.3"
    ),
    dependencyOverrides ++= Seq(
    )
  )
  .configs(Ci)

lazy val `akka-persistence-mongo-rxmongo` = (project in file("rxmongo"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++=
      Seq("reactivemongo", "reactivemongo-akkastream")
        .map("org.reactivemongo" %% _ % "1.0.10" % Compile)
        .map(_.exclude("com.typesafe.akka","akka-actor_2.11")
          .exclude("com.typesafe.akka","akka-actor_2.12")
          .exclude("com.typesafe.akka","akka-actor_2.13")
          .excludeAll(ExclusionRule("org.apache.logging.log4j"))
        )
  )
  .configs(Ci)

lazy val `akka-persistence-mongo-tools` = (project in file("tools"))
  .dependsOn(`akka-persistence-mongo-scala` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % MongoJavaDriverVersion % "compile"
    ),
    publish / skip := true,
  )
  .configs(Ci)

lazy val `akka-persistence-mongo` = (project in file("."))
  .aggregate(`akka-persistence-mongo-common`, `akka-persistence-mongo-rxmongo`, `akka-persistence-mongo-scala`, `akka-persistence-mongo-tools`)
  .settings(
    crossScalaVersions := Nil,
    publish / skip := true,
    publishTo := Some(Resolver.file("file", new File("target/unusedrepo")))
  )
