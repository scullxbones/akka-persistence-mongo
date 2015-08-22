## A MongoDB plugin for [akka-persistence](http://akka.io)

 * Three projects, a core and two driver implementations.  To use you must pull jars for *only one* of the drivers. Common will be pulled in as a transitive dependency:
   * common provides integration with Akka persistence, implementing the plugin API
   * casbah provides an implementation against the casbah driver
   * rxmongo provides an implementation against the ReactiveMongo driver
 * The tests will automatically download mongodb via flapdoodle's embedded mongo utility, do not be alarmed :)
 * Supports Akka 2.4 series
 * Supports MongoDB major versions 2.6 and 3.0
 * Compiled against scala `2.11`.  When `2.12` is released, will cross compile.  Waiting on dependent libraries to catch up 

### Change log is [here](changelog24.md)

### Quick Start

* Choose a driver - Casbah and ReactiveMongo are supported
* Add the following to sbt:

(Casbah)
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-casbah" % "1.0.0-SNAPSHOT"
```
(Reactive Mongo)
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-rxmongo" % "1.0.0-SNAPSHOT"
```
* Inside of your `application.conf` file, add the following line if you want to use the journal (snapshot is optional).  The casbah/rxmongo selection should be pulled in by a `reference.conf` in the driver jar you choose:
```
akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
```

### Details
1. [Major changes in 1.x](#major)
   * [Akka 2.4](#akka24)
   * [Journal Data Model](#model)
   * [0.x Journal Migration](#migration)
   * [Read Journal](#readjournal)
   * [Miscellaneous](#miscchanges)
1. [Configuration Details](#config)

<a name="major"/>
### Major Changes in 1.x
<a name="akka24"/>

#### Akka 2.4 support
* The driving change for this new major version of the library is support of Akka 2.4
* Many things changed with Akka 2.4, not limited to:
  * Scala 2.10 support dropped
  * Java 7 support dropped
  * Journal Plugin API breaking changes
  * Read Journal added as an experimental feature
  * More information in [the migration guide](http://doc.akka.io/docs/akka/snapshot/project/migration-guide-2.3.x-2.4.x.html)

<a name="model"/>
#### Journal Data Model Changes
##### This upgrade gave a good opportunity to correct the data model and make it more robust:

  * Writes are atomic at the batch level, meaning if the plugin is sent 100 events, these are persisted in mongo as a single document.  This is much safer from a mongo perspective, and should be more performant when the plugin is receiving large batches of events.  With this we're able to close a long-standing issue with the old driver: [issue #5](https://github.com/scullxbones/akka-persistence-mongo/issues/5).
  * All documents are versioned, allowing a foothold for backward-compatible schema migration
  * Event metadata is directly represented as part of the document rather than being embedded in an opaque binary representation
  * There is a generic (non-driver specific) data model to improve consistency
  * All payloads have type hints, so that different payload types can be easily supported in the future
  * Because of type hints, new pass-throughs are supported.  Pass-throughs are directly represented as their Bson version for easier inspection:
    * In the 0.x series a pass-through was added for Bson documents (either `DBObject` for casbah or `BSONDocument` for RX); payloads that were not of this type were run through Akka Serialization
    * In the 1.x series, `Array[Byte]`, `String`, `Double`, `Long`, and `Boolean` are added as pass-throughs.  The fallback continues to use Akka Serialization

<a name="migration"/>  
#### Migration of 0.x Journal

* The 1.x journal is backward-incompatible with the 0.x journal.  Here are a couple of options for dealing with this.  These approaches assume no inbound writes to the journal:
  1. Don't care about history? Take snapshots, and delete the journal from 0 to Max Long.
  1. Care about history? Take a mongodump of the collection *FIRST*
    * The driver will attempt to upgrade a 0.x journal to 1.x if the configuration option is set: `akka.contrib.persistence.mongodb.mongo.journal-automatic-upgrade=true`
    * By default this feature is disabled, as it's possible that it can corrupt the journal.  It should be used in a controlled manner
  
<a name="readjournal"/>
#### Read Journal [akka docs](http://doc.akka.io/docs/akka/snapshot/scala/persistence-query.html)

* There is a new experimental module in akka called `akka-persistence-query-experimental`.
  * This provides a way to create an `akka-streams Source` of data from a query posed to the journal plugin
  * The implementation of queries are purely up to the journal developer
  * The `ReadJournal` interface provides plenty of leeway for metadata being exposed as well via `Materialized Values of Queries`
  * This is a very fluid part of `akka-persistence` for the moment, so expect it to be quite unstable
* Initially two queries are supported.  Both are non-live for the moment, and thus would complete when no more records are available:
1. `AllPersistenceIds` (akka standard) - Provides a `Source[String,Unit]` of all of the persistence ids in the journal currently.  The results will be sorted by `persistenceId`.
1. `akka.contrib.persistence.mongodb.MongoReadJournal.AllEvents` (driver specific) - Provides a `Source[EventEnvelope,Unit]` of every event in the journal.  The results will be sorted by `persistenceId` and `sequenceNumber`.
* Eventually i'd like to support live versions of these queries, plus the `EventsByPersistenceId` query.
* I'll look for community feedback about what driver-specific queries might be useful as well

<a name="miscchanges"/>
#### Miscellaneous Other Changes

* The `CircuitBreaker` implementation operates a little differently:
  * Writes operate as before
  * Reads (think replays) do not contribute to timeouts or errors, but if the `CircuitBreaker` is in `Open` state, replays will fail fast.
* Travis now verifies builds and tests run against both Mongo 2.6 and 3.0
* Several metrics were no longer relevant due to journal changes in 2.4
* 

<a name="config"/>
#### Configuration

MORE TO COME
