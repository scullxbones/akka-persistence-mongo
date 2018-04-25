val releaseV = "2.0.8-ffm1"

val scalaV = "2.11.8"

scalaVersion := scalaV

crossScalaVersions := Seq("2.11.8", "2.12.2")

val AkkaV = "2.5.1"

def commonDeps(sv:String) = Seq(
  ("com.typesafe.akka"  %% "akka-persistence" % AkkaV % "provided")
    .exclude("org.iq80.leveldb", "leveldb")
    .exclude("org.fusesource.leveldbjni", "leveldbjni-all"),
  (sv match {
    case "2.11" => "nl.grons"           %% "metrics-scala" % "3.5.5_a2.3"
    case "2.12" => "nl.grons"           %% "metrics-scala" % "3.5.5_a2.4"
  })
    .exclude("com.typesafe.akka", "akka-actor_2.11")
    .exclude("com.typesafe.akka", "akka-actor_2.12"),
  "com.typesafe.akka"         %% "akka-persistence-query"   % AkkaV     % "provided",
  "org.mongodb"               % "mongodb-driver"            % "3.6.3"   % "test",
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

val commonSettings = Seq(
  scalaVersion := scalaV,
  libraryDependencies ++= commonDeps(scalaBinaryVersion.value),
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
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument("-oDS")
)

lazy val `akka-persistence-mongo-common` = (project in file("common"))
  .settings(commonSettings:_*)

lazy val `akka-persistence-mongo-casbah` = (project in file("casbah"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb" %% "casbah" % "3.1.1" % "provided",
      "org.mongodb" %  "mongo-java-driver" % "3.6.3" % "test"
    )
  )

lazy val `akka-persistence-mongo-rxmongo` = (project in file("rxmongo"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      ("org.reactivemongo" %% "reactivemongo" % "0.13.0" % "provided")
        .exclude("com.typesafe.akka","akka-actor_2.11")
        .exclude("com.typesafe.akka","akka-actor_2.12"),
      ("org.reactivemongo" %% "reactivemongo-akkastream" % "0.13.0" % "provided")
        .exclude("com.typesafe.akka","akka-actor_2.11")
        .exclude("com.typesafe.akka","akka-actor_2.12")
    ),
    crossScalaVersions := Seq("2.11.8"),
    scalaVersion := "2.11.8"
  )

lazy val `akka-persistence-mongo-tools` = (project in file("tools"))
  .dependsOn(`akka-persistence-mongo-casbah` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb" %% "casbah" % "3.1.1" % "provided"
    )
  )

lazy val `akka-persistence-mongo` = (project in file("."))
  .aggregate(`akka-persistence-mongo-common`, `akka-persistence-mongo-casbah`, `akka-persistence-mongo-rxmongo`, `akka-persistence-mongo-tools`)
  .settings(commonSettings:_*)
  .settings(
    packagedArtifacts in file(".") := Map.empty,
    publishTo := Some(Resolver.file("file", new File("target/unusedrepo"))))
