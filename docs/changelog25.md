## Changelog for 2.x major version

### 2.3.2
* Upgrade what was possible; fix some scalatest deprecations; scalatest 3.1.0 requires akka to jump first due to recompile requirement
* Clean up outstanding warnings, deprecations, etc
* Update to latest sbt-pgp sbt-sonatype
* Merge pull request #282 from scala-steward/update/sbt-1.3.5
* Merge pull request #284 from scala-steward/update/slf4j-api-1.7.30
* Merge pull request #288 from scala-steward/update/reactivemongo-0.19.5
* Merge pull request #287 from scala-steward/update/mongodb-driver-3.12.3
* Update reactivemongo, ... to 0.19.5
* Update mongodb-driver, ... to 3.12.3
* Merge branch 'master' into update/slf4j-api-1.7.30
* Merge pull request #273 from scala-steward/update/reactivemongo-0.18.8
* Merge pull request #274 from scala-steward/update/metrics4-akka_a24-4.1.1
* Merge pull request #277 from scala-steward/update/mongodb-driver-3.11.2
* Merge pull request #283 from scala-steward/update/log4j-core-2.13.0
* Merge pull request #286 from scala-steward/update/netty-buffer-4.1.44.Final
* Update netty-buffer, netty-handler, ... to 4.1.44.Final
* Update slf4j-api to 1.7.30
* Update log4j-api, log4j-core, ... to 2.13.0
* Update sbt to 1.3.5
* Update mongodb-driver, ... to 3.11.2
* Update metrics4-akka_a24 to 4.1.1
* Update reactivemongo, ... to 0.18.8

### 2.3.1
* Remove unintended for-comp filter sugar
* Improve collections count (#260)
* Merge pull request #264 from scala-steward/update/netty-buffer-4.1.41.Final
* Update netty-buffer, netty-handler, ... to 4.1.41.Final
* Merge pull request #258 from scala-steward/update/slf4j-api-1.7.28
* Merge pull request #252 from scala-steward/update/junit-4.12
* Merge pull request #251 from scala-steward/update/netty-buffer-4.1.39.Final
* Merge pull request #261 from gbrd/pr-use-snaps-cache-in-scala-driver
* Update slf4j-api to 1.7.28
* Merge branch 'master' into update/slf4j-api-1.7.28
* Update junit to 4.12
* Merge branch 'master' into update/junit-4.12
* Update netty-buffer, netty-handler, ... to 4.1.39.Final
* Merge branch 'master' into update/netty-buffer-4.1.39.Final
* Merge pull request #256 from scala-steward/update/reactive-streams-1.0.3
* Merge pull request #254 from scala-steward/update/mockito-all-1.10.19
* Merge pull request #253 from scala-steward/update/log4j-core-2.12.1
* Use snapshot collection cache in scala driver too.
* Update slf4j-api to 1.7.28
* Update reactive-streams to 1.0.3
* Update mockito-all to 1.10.19
* Update log4j-api, log4j-core, ... to 2.12.1
* Update junit to 4.12
* Update netty-buffer, netty-handler, ... to 4.1.39.Final

### 2.3.0
* Merge pull request #247 from WellingR/feature/scala-2.13-support
* Support scala 2.13
* Libary upgrades in preparation for scala 2.13

### 2.2.10
* Clean up some old deprecation and warning messages; reintroduce configured writeconcern
* Reactive Mongo 0.18.4 Compatibility (#244)

### 2.2.9
* improve the way official driver settings are used for connection (#236)

### 2.2.8
* Merge pull request #235 from JeanFrancoisGuena/streamed-currentPersistenceIds
* Source for current Persistence Ids processed in a streamed manner
* Undo overzealous regex changes from pre release script

### 2.2.6
* Suffixed collections scala driver migration (#232)
* Fixes for cross-build and publishing from sbt 1.x upgrade
* Fixes related to sbt 1.x upgrade

### 2.2.5
* Use Akka DynamicAccess for reflective classloading (#230)

### 2.2.4
* Improve collection creation for long-running systems under high load (#225)
* Document the minimum akka 2.5 version

### 2.2.3
* Merge pull request #223 from bsinno/bugfix/213
* Merge branch 'master' into bugfix/213
* Merge pull request #222 from scullxbones/wip-219
* Issue #219 - IDs should match between journal & realtime
* recover from NamespaceExists in ScalaDriverPE.ensureCollection

### 2.2.1
* Allows to provide a custom MetricsBuilder (#211)
* Merge pull request #212 from bsinno/optimize-snapshot-collection-cache
* Merge branch 'master' into optimize-snapshot-collection-cache
* Optimization: snapshot collection cache

### 2.2.0
* Merge pull request #208 from scullxbones/wip-184
* Fix off-by-1 in casbah; update official scala to support ensureCollection
* Try rxm 0.16 against latest master
* Bump to reactive-mongo 0.15.1
* Use the defined failoverStrategy for authenticated connection
* Add akka.test.timefactor=3 option by default to tests
* Re-enable parallel-execution
* Use new ensureCollection method in MongoPersistenceExtension
* Add an 'ensureCollection' method in MongoPersistenceExtension to be implemented in subclasses
* JournalTckSpec: Run the DB cleanup after all the tests rather than before
* Fix compilation error due to API change after upgrading to rxmongo 0.15
* Upgraded to reactivemongo 0.15
* Updated docker conf to delete the containers
* Add mongodb official scala driver support (#207)

### 2.1.1
* fix: Race condition on deleteFrom #203 (#205)
* fix: replace fix #179 with akka/akka #24321 (#206)

### 2.1.0
* Change live queries to be directly connected to database (#202)

### 2.0.12
* Merge pull request #201 from scullxbones/wip-200
* Fix for case when query is run before collections exist

### 2.0.11
* Fix for RXM tag query being too broad (#196)

### 2.0.10
* Merge pull request #192 from Fabszn/externalizedConfBuffer
* Makes buffer size can be setting up from properties

### 2.0.9
* Merge pull request #189 from bmontuelle/master
* Filter database system collections
* Fix regex substitute error; upgrade to 0.13.10 due to `sbt-dependency-graph`

### 2.0.8
* Merge pull request #187 from TiendaNube/fix-two-issues
* Fix#1 - Filter out realtime collection name when using multiple collections. Fix#2 - Use the configuration to enable/disable the realtime cursor.
* Still trying to clean up the release process

### 2.0.7
* Wip smoke tests against 3.6 (#183)
* Break release process into two parts

### 2.0.6
* Optimization: journal collection cache (#181) - Thanks to @gbrd for the PR

### 2.0.5
* ActorRef serialization, quite old bug. transportInformation should be set.  Thanks to @gbrd for the PR
[#179](https://github.com/scullxbones/akka-persistence-mongo/issues/179)

### 2.0.4
* Remove circuit breaker - currently redundant with breaker provided by akka-persistence layer [#168](https://github.com/scullxbones/akka-persistence-mongo/issues/168)
* Properly document rxmongo akka-stream requirement [#166](https://github.com/scullxbones/akka-persistence-mongo/issues/166)
* Don't incur cost of extra `createIndices` calls for single-collection (default) configuration [#169](https://github.com/scullxbones/akka-persistence-mongo/issues/169)

### 2.0.3
* Add tagging/query by tag support; make some strides on repeatably passing tests in CI by running project tests in serial
[#37](https://github.com/scullxbones/akka-persistence-mongo/issues/37)

### 2.0.2
* Retry fixing 151
[#151](https://github.com/scullxbones/akka-persistence-mongo/issues/151)

### 2.0.1
* Fix for timing-based error upon deleting journal entries [#151](https://github.com/scullxbones/akka-persistence-mongo/issues/151)

### 2.0.0
* Initial support of akka 2.5
