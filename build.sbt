val releaseV = "2.3.0"

val scala211V = "2.11.12"
val scala212V = "2.12.9"
val scala213V = "2.13.0"

val scalaV = scala211V


val LegacyAkkaV = "2.5.12" //min version to have Serialization.withTransportInformation
val LatestAkkaV = "2.5.25"
def akkaV(sv: String): String = sv match {
  case "2.11" => LegacyAkkaV
  case _ => LatestAkkaV
}

val MongoJavaDriverVersion = "3.11.0"

def commonDeps(sv:String) = Seq(
  ("com.typesafe.akka"  %% "akka-persistence" % akkaV(sv))
    .exclude("org.iq80.leveldb", "leveldb")
    .exclude("org.fusesource.leveldbjni", "leveldbjni-all"),
  (sv match {
    case "2.11"          => "nl.grons" %% "metrics4-akka_a24" % "4.0.8"
    case "2.12" | "2.13" => "nl.grons" %% "metrics4-akka_a25" % "4.0.8"
  })
    .exclude("com.typesafe.akka", "akka-actor_2.11")
    .exclude("com.typesafe.akka", "akka-actor_2.12")
    .exclude("com.typesafe.akka", "akka-actor_2.13"),
  "com.typesafe.akka"         %% "akka-persistence-query"   % akkaV(sv) % "compile",
  "com.typesafe.akka"         %% "akka-persistence"         % akkaV(sv) % "compile",
  "com.typesafe.akka"         %% "akka-actor"               % akkaV(sv) % "compile",
  "org.mongodb"               % "mongodb-driver-core"       % MongoJavaDriverVersion   % "compile",
  "org.mongodb"               % "mongodb-driver"            % MongoJavaDriverVersion   % "test",
  "org.slf4j"                 % "slf4j-api"                 % "1.7.22"  % "test",
  "org.apache.logging.log4j"  % "log4j-api"                 % "2.12.1"     % "test",
  "org.apache.logging.log4j"  % "log4j-core"                % "2.12.1"     % "test",
  "org.apache.logging.log4j"  % "log4j-slf4j-impl"          % "2.12.1"     % "test",
  "org.scalatest"             %% "scalatest"                % "3.0.8"   % "test",
  "junit"                     % "junit"                     % "4.11"    % "test",
  "org.mockito"               % "mockito-all"               % "1.10.19"   % "test",
  "com.typesafe.akka"         %% "akka-slf4j"               % akkaV(sv) % "test",
  "com.typesafe.akka"         %% "akka-testkit"             % akkaV(sv) % "test",
  "com.typesafe.akka"         %% "akka-persistence-tck"     % akkaV(sv) % "test",
  "com.typesafe.akka"         %% "akka-cluster-sharding"    % akkaV(sv) % "test"
)

lazy val Travis = config("travis").extend(Test)

ThisBuild / organization := "com.github.scullxbones"
ThisBuild / version      := releaseV
ThisBuild / scalaVersion := scalaV

val commonSettings = Seq(
  scalaVersion := scalaV,
  crossScalaVersions := Seq(scala211V, scala212V, scala213V),
  libraryDependencies ++= commonDeps(scalaBinaryVersion.value),
  dependencyOverrides ++= Seq(
    "com.typesafe" % "config" % "1.3.2",
    "org.slf4j" % "slf4j-api" % "1.7.28",
    "com.typesafe.akka" %% "akka-stream" % akkaV(scalaBinaryVersion.value),
    "org.mongodb" % "mongo-java-driver" % MongoJavaDriverVersion
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
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
    "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  ),
  parallelExecution in Test := true,
  testOptions in Test += Tests.Argument("-oDS"),
  testOptions in Travis += Tests.Argument("-l", "org.scalatest.tags.Slow"),
  fork in Test := true,
  publishTo := sonatypePublishTo.value,
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
) ++ inConfig(Travis)(Defaults.testTasks)

lazy val `akka-persistence-mongo-common` = (project in file("common"))
  .settings(commonSettings:_*)
  .configs(Travis)

lazy val `akka-persistence-mongo-casbah` = (project in file("casbah"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(crossScalaVersions := Seq(scala211V, scala212V)) // not available for 2.13
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb" %% "casbah" % "3.1.1" % "compile"
    )
  )
  .configs(Travis)

lazy val `akka-persistence-mongo-scala` = (project in file("scala"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.7.0"        % "compile",
      "org.mongodb.scala" %% "mongo-scala-bson"   % "2.7.0"        % "compile",
      "io.netty"          % "netty-buffer"        % "4.1.17.Final" % "compile",
      "io.netty"          % "netty-transport"     % "4.1.17.Final" % "compile",
      "io.netty"          % "netty-handler"       % "4.1.17.Final" % "compile",
      "org.reactivestreams" % "reactive-streams"  % "1.0.3"
    )
  )
  .configs(Travis)

lazy val `akka-persistence-mongo-rxmongo` = (project in file("rxmongo"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++=
      Seq("reactivemongo", "reactivemongo-akkastream")
        .map("org.reactivemongo" %% _ % "0.18.4" % "compile")
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
  .aggregate(`akka-persistence-mongo-common`, `akka-persistence-mongo-casbah`, `akka-persistence-mongo-rxmongo`, `akka-persistence-mongo-scala`, `akka-persistence-mongo-tools`)
  .settings(
    crossScalaVersions := Nil,
    skip in publish := true,
    publishTo := Some(Resolver.file("file", new File("target/unusedrepo")))
  )
