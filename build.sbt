val releaseV = "3.0.6"

val scala212V = "2.12.14"
val scala213V = "2.13.2"

val scalaV = scala213V
val akkaV = "2.6.15"

val MongoJavaDriverVersion = "4.3.1"

val commonDeps = Seq(
  ("com.typesafe.akka"  %% "akka-persistence" % akkaV)
    .exclude("org.iq80.leveldb", "leveldb")
    .exclude("org.fusesource.leveldbjni", "leveldbjni-all"),
  ("nl.grons" %% "metrics4-akka_a25" % "4.1.19")
    .exclude("com.typesafe.akka", "akka-actor_2.11")
    .exclude("com.typesafe.akka", "akka-actor_2.12")
    .exclude("com.typesafe.akka", "akka-actor_2.13"),
  "com.typesafe.akka"         %% "akka-persistence-query"   % akkaV     % "compile",
  "com.typesafe.akka"         %% "akka-persistence"         % akkaV     % "compile",
  "com.typesafe.akka"         %% "akka-actor"               % akkaV     % "compile",
  "org.mongodb"               % "mongodb-driver-core"       % MongoJavaDriverVersion   % "compile",
  "org.mongodb"               % "mongodb-driver-legacy"     % MongoJavaDriverVersion   % "test",
  "org.slf4j"                 % "slf4j-api"                 % "1.7.32"  % "test",
  "org.apache.logging.log4j"  % "log4j-api"                 % "2.14.1"  % "test",
  "org.apache.logging.log4j"  % "log4j-core"                % "2.14.1"  % "test",
  "org.apache.logging.log4j"  % "log4j-slf4j-impl"          % "2.14.1"  % "test",
  "org.scalatest"             %% "scalatest"                % "3.2.3"   % "test",
  "org.scalatestplus"         %% "mockito-1-10"             % "3.1.0.0" % "test",
  "org.scalatestplus"         %% "junit-4-12"               % "3.2.2.0" % "test",
  "junit"                     % "junit"                     % "4.13.1"    % "test",
  "org.mockito"               % "mockito-all"               % "1.10.19" % "test",
  "com.typesafe.akka"         %% "akka-slf4j"               % akkaV     % "test",
  "com.typesafe.akka"         %% "akka-testkit"             % akkaV     % "test",
  "com.typesafe.akka"         %% "akka-persistence-tck"     % akkaV     % "test",
  "com.typesafe.akka"         %% "akka-cluster-sharding"    % akkaV     % "test"
)

lazy val Travis = config("travis").extend(Test)

ThisBuild / organization := "com.github.scullxbones"
ThisBuild / version      := releaseV
ThisBuild / scalaVersion := scalaV

val commonSettings = Seq(
  scalaVersion := scalaV,
  crossScalaVersions := Seq(scala212V, scala213V),
  libraryDependencies ++= commonDeps,
  dependencyOverrides ++= Seq(
    "com.typesafe" % "config" % "1.3.2",
    "org.slf4j" % "slf4j-api" % "1.7.32",
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "org.mongodb" % "mongodb-driver-legacy" % MongoJavaDriverVersion
  ),
  version := releaseV,
  organization := "com.github.scullxbones",
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
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument("-oDS"),
  testOptions in Travis += Tests.Argument("-l", "org.scalatest.tags.Slow"),
  fork in Test := false,
  publishTo := sonatypePublishTo.value,
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
) ++ inConfig(Travis)(Defaults.testTasks)

lazy val `akka-persistence-mongo-common` = (project in file("common"))
  .settings(commonSettings:_*)
  .configs(Travis)

lazy val `akka-persistence-mongo-scala` = (project in file("scala"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % "4.3.1"        % "compile",
      "org.mongodb.scala" %% "mongo-scala-bson"   % "4.3.1"        % "compile",
      "io.netty"          % "netty-buffer"        % "4.1.67.Final" % "compile",
      "io.netty"          % "netty-transport"     % "4.1.67.Final" % "compile",
      "io.netty"          % "netty-handler"       % "4.1.67.Final" % "compile",
      "org.reactivestreams" % "reactive-streams"  % "1.0.3"
    ),
    dependencyOverrides ++= Seq(
    )
  )
  .configs(Travis)

lazy val `akka-persistence-mongo-rxmongo` = (project in file("rxmongo"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++=
      Seq("reactivemongo", "reactivemongo-akkastream")
        .map("org.reactivemongo" %% _ % "1.0.6" % Compile)
        .map(_.exclude("com.typesafe.akka","akka-actor_2.11")
          .exclude("com.typesafe.akka","akka-actor_2.12")
          .exclude("com.typesafe.akka","akka-actor_2.13")
          .excludeAll(ExclusionRule("org.apache.logging.log4j"))
        )
  )
  .configs(Travis)

lazy val `akka-persistence-mongo-tools` = (project in file("tools"))
  .dependsOn(`akka-persistence-mongo-scala` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.7.0" % "compile"
    )
  )
  .configs(Travis)

lazy val `akka-persistence-mongo` = (project in file("."))
  .aggregate(`akka-persistence-mongo-common`, `akka-persistence-mongo-rxmongo`, `akka-persistence-mongo-scala`, `akka-persistence-mongo-tools`)
  .settings(
    crossScalaVersions := Nil,
    skip in publish := true,
    publishTo := Some(Resolver.file("file", new File("target/unusedrepo")))
  )
