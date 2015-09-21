## Changelog for 1.x major version

### 1.0.5

* Fix #46 - not properly handling streaming of read journal queries; use cursors and/or `Enumerator` for rxm
* Fix #47 - update to (and recompile against) 2.4.0-RC3 including changes to read journals

### 1.0.4

* Remove legacy akka-cluster-sharding records during journal upgrade

### 1.0.3

* Remove legacy index during journal upgrade

### 1.0.2

* More logging around automated journal upgrade

### 1.0.1

* Add logging around automated journal upgrade to better understand what is happening
