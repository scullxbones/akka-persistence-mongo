## Changelog for 1.x major version

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
