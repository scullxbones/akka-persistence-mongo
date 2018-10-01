## Changelog for 2.x major version

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
