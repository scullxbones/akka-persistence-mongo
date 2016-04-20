## Changelog for 1.x major version

### 1.2.4
* Issue [#114](https://github.com/scullxbones/akka-persistence-mongo/issues/114)
  * Fix for rxmongo correct sequencing of realtime inserts to eliminate holes in PersistentQueries - thanks @marcuslinke!
* Issue [#113](https://github.com/scullxbones/akka-persistence-mongo/issues/113)
  * Fix for `EventsByPersistenceId` queries to respect from/to sequence numbers

### 1.2.3
* Issue [#111](https://github.com/scullxbones/akka-persistence-mongo/issues/111)
  * Fix for rxmongo loading all of a single persistent actor's events into memory - thanks @marcuslinke!

### 1.2.2
* Issue [#100](https://github.com/scullxbones/akka-persistence-mongo/issues/100)
  * Provides support for very large number of `persistenceId`s without hitting mongodb document size limit
* Issue [#108](https://github.com/scullxbones/akka-persistence-mongo/issues/108):
  * Uses upsert rather than insert on snapshots to replace latest snapshot when snapshotting faster than 1 per millisecond

### 1.2.1
* Update to be compatible with ReactiveMongo from 0.11.8 - 0.11.10
* Enable serializable checking in TCK

### 1.2.0
* Now tested compatible with Mongo `3.2`,`3.0`,`2.6`
* Issue [#96](https://github.com/scullxbones/akka-persistence-mongo/issues/91) updates ReactiveMongo to recent `0.11` version
* Issue [#92](https://github.com/scullxbones/akka-persistence-mongo/issues/92) update to support Akka 2.4.2 (really support new non-experimental streams API)
* Merges in [#99](https://github.com/scullxbones/akka-persistence-mongo/pull/99) thanks @marekzebrowski!

### 1.1.11
* Issue [#91](https://github.com/scullxbones/akka-persistence-mongo/issues/91) was reopened - this release fixes and closes that issue

### 1.1.10
* Fixes issue [#93](https://github.com/scullxbones/akka-persistence-mongo/issues/93):
  * Casbah driver had incorrect `events:{$size:0}` query, causing no records to be matched or deleted
* Add prettier report to logs of load spec runs
* Make load spec async to significant benefit of casbah and detriment of rxmongo (needs further investigation)

### 1.1.9
* Fixes issue [#91](https://github.com/scullxbones/akka-persistence-mongo/issues/91):
  * RxMongo driver re-uses `MongoDriver` instance, cutting down on amount of resources used when more than journal or snapshot is used
  * RxMongo was not batching writes together - fix for performance boost under `persistAsync` scenarios
  * Remove `no timeout` option from `CurrentAllEvents` read journal query

### 1.1.8
* Fixes issue [#89](https://github.com/scullxbones/akka-persistence-mongo/issues/89):
  * Change sort to query that will leverage index
  * Fix inconsistencies found in read journals, rxm driver

### 1.1.7
* Fixes issue [#88](https://github.com/scullxbones/akka-persistence-mongo/issues/88):
  * For Casbah, journal entry query not correctly specifying sequence number bounds
  * Some more internal metric refactoring
  * Merges in - thanks @HannesSchr!:
    * PR[#86](https://github.com/scullxbones/akka-persistence-mongo/pull/86)
    * PR[#87](https://github.com/scullxbones/akka-persistence-mongo/pull/87)

### 1.1.6
* Issue [#84](https://github.com/scullxbones/akka-persistence-mongo/issues/84) addressed:
  * Adds ability to override/set failover settings for rxmongo driver
  * Updates to clean up metrics names

### 1.1.5
* PR [#82](https://github.com/scullxbones/akka-persistence-mongo/pull/82) merged:
  * Allows naming index used for finding the maximum sequence # for a given `persistenceId` - thanks @bilyush!

### 1.1.4
* Issue [#79](https://github.com/scullxbones/akka-persistence-mongo/issues/79) fixed 
  * Fix unindexed query causing issues on large journals by adding additional index
 
### 1.1.3
* PR [#71](https://github.com/scullxbones/akka-persistence-mongo/pull/71) merged ~ thanks @matheuslima:
  * Add support for allEvents/currentEvents and allPersistenceIds/currentPersistenceIds queries
* Also some refactoring and changes to try and address instability in RxMongo read journal - still a WIP

### 1.1.2
* For casbah, must drop old index by name during journal upgrade process

### 1.1.1
* Correctly ordered upgrade (drop old index, upgrade, add new index) fixes issue [#70](https://github.com/scullxbones/akka-persistence-mongo/issues/70)

### 1.1.0
* PR [#69](https://github.com/scullxbones/akka-persistence-mongo/pull/69) merged:
  * Adds support for eventsByPersistenceId query

### 1.0.11
* PR [#67](https://github.com/scullxbones/akka-persistence-mongo/pull/67) merged:
  * Adds support for separate credential-storing and data-storing databases
  * Thanks to @LeightonWong for the contribution!
* Add some `require` assertions so as to fail quickly when null `persistenceId` is passed to `EventsByPersistenceId` query

### 1.0.10

* Fix [#60](https://github.com/scullxbones/akka-persistence-mongo/issues/60):
  * Adds configuration option to force legacy serialization, fully delegating to `akka-serialization` for payloads
  * Stores entire `PersistentRepr` as a unit in payload with _type = "repr"
* Fix [#62](https://github.com/scullxbones/akka-persistence-mongo/issues/62).  Was still using `foldWhile` for ReactiveMongo journal upgrade - this method seems to stop iteration at record #100.  Iteratees have no such issue - switch to Iteratees

### 1.0.9

* Fix [#52](https://github.com/scullxbones/akka-persistence-mongo/issues/52) - Also fixes #45, #48 (indirectly).  Default to using `Thread.getContextClassLoader` if it is set, otherwise fall back to `Class.getClassloader`.  Should allow the plugin to play nicer with `Playframework` apps (e.g. `activator run`)

### 1.0.8

* Fix [#57](https://github.com/scullxbones/akka-persistence-mongo/issues/57) - Dump `foldWhile` for play `Iteratee`s, not sure if issue is upstream or local.  Thanks to @matheuslima for helping find a reproducing test case 

### 1.0.7

* Address [#54](https://github.com/scullxbones/akka-persistence-mongo/issues/54) - Remove bulk inserts from reactive mongo - appears to have an issue upstream

### 1.0.6

* Fix [#43](https://github.com/scullxbones/akka-persistence-mongo/issues/43) - Support multiple concurrently running plugins with different configurations

### 1.0.5

* Fix [#46](https://github.com/scullxbones/akka-persistence-mongo/issues/46) - not properly handling streaming of read journal queries; use cursors and/or `Enumerator` for rxm
* Fix [#47](https://github.com/scullxbones/akka-persistence-mongo/issues/47) - update to (and recompile against) 2.4.0-RC3 including changes to read journals

### 1.0.4

* Remove legacy akka-cluster-sharding records during journal upgrade

### 1.0.3

* Remove legacy index during journal upgrade

### 1.0.2

* More logging around automated journal upgrade

### 1.0.1

* Add logging around automated journal upgrade to better understand what is happening
