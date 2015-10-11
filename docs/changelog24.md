## Changelog for 1.x major version

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
