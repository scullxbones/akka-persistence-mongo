val releaseV = "2.2.5"

val scalaV = "2.11.12"

scalaVersion := scalaV

crossScalaVersions := Seq("2.11.12", "2.12.8")

val AkkaV = "2.5.12" //min version to have Serialization.withTransportInformation
val MongoJavaDriverVersion = "3.8.2"

def commonDeps(sv:String) = Seq(
  ("com.typesafe.akka"  %% "akka-persistence" % AkkaV)
    .exclude("org.iq80.leveldb", "leveldb")
    .exclude("org.fusesource.leveldbjni", "leveldbjni-all"),
  (sv match {
    case "2.11" => "nl.grons"           %% "metrics-scala" % "3.5.5_a2.3"
    case "2.12" => "nl.grons"           %% "metrics-scala" % "3.5.5_a2.4"
  })
    .exclude("com.typesafe.akka", "akka-actor_2.11")
    .exclude("com.typesafe.akka", "akka-actor_2.12"),
  "com.typesafe.akka"         %% "akka-persistence-query"   % AkkaV     % "compile",
  "org.mongodb"               % "mongodb-driver-core"       % MongoJavaDriverVersion   % "compile",
  "org.mongodb"               % "mongodb-driver"            % MongoJavaDriverVersion   % "test",
  "org.slf4j"                 % "slf4j-api"                 % "1.7.22"  % "test",
  "org.apache.logging.log4j"  % "log4j-api"                 % "2.5"     % "test",
  "org.apache.logging.log4j"  % "log4j-core"                % "2.5"     % "test",
  "org.apache.logging.log4j"  % "log4j-slf4j-impl"          % "2.5"     % "test",
  "org.scalatest"             %% "scalatest"                % "3.0.1"   % "test",
  "junit"                     % "junit"                     % "4.11"    % "test",
  "org.mockito"               % "mockito-all"               % "1.9.5"   % "test",
  "com.typesafe.akka"         %% "akka-slf4j"               % AkkaV     % "test",
  "com.typesafe.akka"         %% "akka-testkit"             % AkkaV     % "test",
  "com.typesafe.akka"         %% "akka-persistence-tck"     % AkkaV     % "test",
  "com.typesafe.akka"         %% "akka-cluster-sharding"    % AkkaV     % "test"
)

lazy val Travis = config("travis").extend(Test)

val commonSettings = Seq(
  scalaVersion := scalaV,
  dependencyOverrides += "org.mongodb" % "mongodb-driver" % "3.8.2" ,
  libraryDependencies ++= commonDeps(scalaBinaryVersion.value),
  dependencyOverrides ++= Seq(
    "com.typesafe" % "config" % "1.3.2",
    "org.slf4j" % "slf4j-api" % "1.7.22",
    "com.typesafe.akka" %% "akka-stream" % AkkaV,
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
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.2"        % "compile",
      "org.mongodb.scala" %% "mongo-scala-bson"   % "2.4.2"        % "compile",
      "io.netty"          % "netty-buffer"        % "4.1.17.Final" % "compile",
      "io.netty"          % "netty-transport"     % "4.1.17.Final" % "compile",
      "io.netty"          % "netty-handler"       % "4.1.17.Final" % "compile",
      "org.reactivestreams" % "reactive-streams"  % "1.0.2"
    )
  )
  .configs(Travis)

lazy val `akka-persistence-mongo-rxmongo` = (project in file("rxmongo"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      ("org.reactivemongo" %% "reactivemongo" % "0.16.0" % "compile")
        .exclude("com.typesafe.akka","akka-actor_2.11")
        .exclude("com.typesafe.akka","akka-actor_2.12"),
      ("org.reactivemongo" %% "reactivemongo-akkastream" % "0.16.0" % "compile")
        .exclude("com.typesafe.akka","akka-actor_2.11")
        .exclude("com.typesafe.akka","akka-actor_2.12")
    ),
    crossScalaVersions := Seq("2.11.8"),
    scalaVersion := "2.11.8"
  )
  .configs(Travis)

lazy val `akka-persistence-mongo-tools` = (project in file("tools"))
  .dependsOn(`akka-persistence-mongo-casbah` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb" %% "casbah" % "3.1.1" % "compile"
    )
  )
  .configs(Travis)

lazy val `akka-persistence-mongo` = (project in file("."))
  .aggregate(`akka-persistence-mongo-common`, `akka-persistence-mongo-casbah`, `akka-persistence-mongo-rxmongo`, `akka-persistence-mongo-scala`, `akka-persistence-mongo-tools`)
  .settings(commonSettings:_*)
  .settings(
    skip in publish := true,
    publishTo := Some(Resolver.file("file", new File("target/unusedrepo")))
  )
  .configs(Travis)
