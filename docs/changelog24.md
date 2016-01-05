## Changelog for 1.x major version

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
