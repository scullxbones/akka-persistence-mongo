## Changelog for 3.x major version

### 3.0.8
* Merge branch 'master' into update/netty-buffer-4.1.72.Final
* Update reactivemongo, ... to 1.0.10
* Update sbt to 1.5.7
* Update mongo-scala-bson, ... to 4.3.4
* Update mongodb-driver-core, ... to 4.3.4
* Update scala-library to 2.13.7
* Update akka-actor, akka-cluster-sharding, ... to 2.6.17
* Update log4j-api, log4j-core, ... to 2.17.0
* Update netty-buffer, netty-handler, ... to 4.1.72.Final
* Convert release process pre over to linux from osx

### 3.0.7
* Transition from TravisCI to Github Actions (#478)
* Update sbt-sonatype to 3.9.10 (#467)
* Update scalatest to 3.2.10 (#473)
* Update scala-compiler, scala-library to 2.12.15 (#472)
* Update mongodb-driver-core, ... to 4.3.2 (#470)
* Update mongo-scala-bson, ... to 4.3.2 (#469)
* Update reactivemongo, ... to 1.0.7 (#468)
* Update akka-actor, akka-cluster-sharding, ... to 2.6.16 (#466)
* Update sbt to 1.5.5
* Update junit to 4.13.2
* Update sbt to 1.4.9
* Update scala-library to 2.13.6 (#448)
* Update scalatest to 3.2.9 (#444)
* Update slf4j-api to 1.7.32 (#455)
* Update netty-buffer to 4.1.67.Final (#462)
* Update netty-handler, netty-transport to 4.1.67.Final
* Do not override driver with an older version
* Fix for changes to scala driver
* Update travis build status links
* Update sbt-pgp to 2.1.1
* Update scala-library to 2.12.14
* Update sbt-sonatype to 3.9.9
* Update akka-actor, akka-cluster-sharding, ... to 2.6.15
* Update reactivemongo, ... to 1.0.6
* Update mongo-scala-bson, ... to 4.3.1
* Update mongodb-driver-core, ... to 4.3.1
* Update netty-handler, netty-transport to 4.1.66.Final
* Update metrics4-akka_a25 to 4.1.19
* Update netty-handler, netty-transport to 4.1.61.Final
* Update log4j-api, log4j-core, ... to 2.14.1
* Update mongo-scala-bson, ... to 4.1.2
* Update mongodb-driver-legacy to 4.1.2

### 3.0.5
* Merge pull request #390 from scullxbones/wip-mongobson-upgade
* Upgrade mongo-bson to latest version

### 3.0.4
* Merge pull request #358 from scala-steward/update/sbt-sonatype-3.9.4
* Merge pull request #381 from scala-steward/update/akka-actor-2.6.9
* Merge pull request #379 from scala-steward/update/netty-buffer-4.1.52.Final
* Merge pull request #376 from scala-steward/update/junit-4-12-3.2.2.0
* Merge pull request #375 from scala-steward/update/scalatest-3.2.2
* Merge pull request #374 from scala-steward/update/reactivemongo-akkastream-1.0.0-rc.3
* Merge branch 'master' into update/reactivemongo-akkastream-1.0.0-rc.3
* Merge pull request #369 from scala-steward/update/mongodb-driver-3.12.7
* Merge pull request #380 from cchantep/task/upd-rm-1.0.0
* Update akka-actor, akka-cluster-sharding, ... to 2.6.9
* Update ReactiveMongo to 1.0.0
* Update netty-buffer, netty-handler, ... to 4.1.52.Final
* Update junit-4-12 to 3.2.2.0
* Update scalatest to 3.2.2
* Update reactivemongo-akkastream to 1.0.0-rc.3
* Update mongodb-driver, ... to 3.12.7
* Update sbt-sonatype to 3.9.4

### 3.0.3
* Merge pull request #360 from scala-steward/update/mongodb-driver-3.12.6
* Merge pull request #364 from scala-steward/update/akka-actor-2.6.8
* Merge pull request #362 from scala-steward/update/netty-buffer-4.1.51.Final
* Merge pull request #352 from scala-steward/update/metrics4-akka_a25-4.1.9
* Merge pull request #355 from scala-steward/update/junit-4-12-3.2.0.0
* Merge pull request #354 from scala-steward/update/scalatest-3.2.0
* Merge pull request #365 from cchantep/task/upd-rm-1.0.0-rc.2
* Update ReactiveMongo to 1.0.0-rc.2
* Update akka-actor, akka-cluster-sharding, ... to 2.6.8
* Update netty-buffer, netty-handler, ... to 4.1.51.Final
* Update mongodb-driver, ... to 3.12.6
* Update junit-4-12 to 3.2.0.0
* Update scalatest to 3.2.0
* Update metrics4-akka_a25 to 4.1.9

### 3.0.2
* Merge pull request #344 from scala-steward/update/log4j-core-2.13.3
* Merge pull request #342 from scala-steward/update/netty-buffer-4.1.50.Final
* Merge branch 'master' into update/log4j-core-2.13.3
* Merge pull request #343 from scala-steward/update/junit-4-12-3.1.2.0
* Merge pull request #339 from scala-steward/update/scalatest-3.1.2
* Merge pull request #337 from scala-steward/update/mongodb-driver-3.12.4
* Merge pull request #340 from JeanFrancoisGuena/master
* Update log4j-api, log4j-core, ... to 2.13.3
* Update junit-4-12 to 3.1.2.0
* Update netty-buffer, netty-handler, ... to 4.1.50.Final
* Try to fix #338 issue: clearing metadata is optional now
* Update scalatest to 3.1.2
* Update mongodb-driver, ... to 3.12.4

### 3.0.1
* Update scalatest and akka in-sync
* Merge pull request #291 from scala-steward/update/reactivemongo-0.19.7
* Merge pull request #292 from scala-steward/update/junit-4.13
* Merge pull request #325 from scala-steward/update/sbt-1.3.10
* Merge pull request #323 from scala-steward/update/mongodb-driver-3.12.3
* Merge pull request #326 from scala-steward/update/netty-buffer-4.1.49.Final
* Merge pull request #327 from scala-steward/update/log4j-core-2.13.2
* workaround for DocumentDB to retrieve maxSequenceNr without sorting on the compound index (#329)
* Update log4j-api, log4j-core, ... to 2.13.2
* Update netty-buffer, netty-handler, ... to 4.1.49.Final
* Update sbt to 1.3.10
* Update mongodb-driver, ... to 3.12.3
* Fixes for release process hiccups
* Prepare for 3.0.0 release
* Update junit to 4.13
* Merge branch 'master' into update/junit-4.13
* Upgrade to akka 2.6 (#301)
* Update junit to 4.13
* Update reactivemongo, ... to 0.19.7

### 3.0.0
* Initial support of akka 2.6
* Eliminate casbah as supported driver
* Limit verified mongo server versions to 3.6+
* Eliminate scala `2.11` as a supported version

