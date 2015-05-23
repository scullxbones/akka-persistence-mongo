val scalaV = "2.11.6"

scalaVersion := scalaV

val AkkaV = "2.3.9"

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
  ("com.typesafe.akka" %% "akka-persistence-experimental" % AkkaV % "provided")
    .exclude("org.iq80.leveldb", "leveldb")
    .exclude("org.fusesource.leveldbjni", "leveldbjni-all"),
  "nl.grons" %% "metrics-scala" % "3.3.0_a2.3",
  "org.scalatest" %% "scalatest" % "2.1.7" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "com.github.simplyscala" %% "scalatest-embedmongo" % "0.2.2" % "test",
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.46.4" % "test",
  "org.mongodb" % "mongo-java-driver" % "2.12.4" % "test",
  "com.typesafe.akka" %% "akka-testkit" % AkkaV % "test",
  "com.typesafe.akka" %% "akka-persistence-tck-experimental" % AkkaV % "test"
)

val commonSettings = Seq(
  scalaVersion := scalaV,
  libraryDependencies ++= commonDeps,
  crossScalaVersions := Seq("2.10.5", "2.11.6"),
  version := "0.3.0-SNAPSHOT",
  organization := "com.github.scullxbones",
  pomExtra := pomXtra,
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  resolvers ++= Seq(
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
    "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )
) ++ sonatypeSettings

lazy val `akka-persistence-mongo-common` = (project in file("common"))
  .settings(commonSettings:_*)

lazy val `akka-persistence-mongo-casbah` = (project in file("casbah"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.mongodb" %% "casbah" % "2.7.4" % "provided"
    )
  )

lazy val `akka-persistence-mongo-rxmongo` = (project in file("rxmongo"))
  .dependsOn(`akka-persistence-mongo-common` % "test->test;compile->compile")
  .settings(commonSettings:_*)
  .settings(
    libraryDependencies ++= Seq(
      "org.reactivemongo" %% "reactivemongo" % "0.10.5.0.akka23" % "provided"
    )
  )

lazy val root = (project in file("."))
  .aggregate(`akka-persistence-mongo-common`, `akka-persistence-mongo-casbah`, `akka-persistence-mongo-rxmongo`)
  .settings(commonSettings:_*)
  .settings(
    packagedArtifacts in file(".") := Map.empty,
    publishTo := Some(Resolver.file("file", new File("target/unusedrepo"))))