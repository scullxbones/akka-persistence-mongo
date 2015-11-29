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

* Choose a driver - Casbah and ReactiveMongo are currently supported
* Driver dependencies are `provided`, meaning they must be included in the application project's dependencies.
* Add the following to sbt:

(Casbah)
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-casbah" % "1.0.11"
```
(Reactive Mongo)
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-rxmongo" % "1.0.11"
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
   * [Mongo URI](#mongouri)
   * [Collection and Index](#mongocollection)
   * [Write Concerns](#writeconcern)
   * [Circuit Breaker / Fail Fast](#circuitbreaker)
   * [Dispatcher](#dispatcher)
   * [Pass-Through BSON](#passthru)
   * [Legacy Serialization](#legacyser)
   * [Metrics](#metrics)
   * [Multiple plugins](#multiplugin)

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
    * Additional logging has been added to help diagnose issues
    * Thanks to @alari's help, if you are:
      1. Having trouble upgrading 
      1. Use cluster-sharding
      1. Have persistent records in your journal for the same from 2.3
    * The automated upgrade process will remove these records including logging their status
      * Refer to [issue 44](https://github.com/scullxbones/akka-persistence-mongo/issues/44) for more details
    * An alternative to removing these records is supplied with akka as of `2.4.0-RC3` [see more details](http://doc.akka.io/docs/akka/2.4.0-RC3/scala/cluster-sharding.html#Removal_of_Internal_Cluster_Sharding_Data)
  
<a name="readjournal"/>
#### Read Journal [akka docs](http://doc.akka.io/docs/akka/snapshot/scala/persistence-query.html)

To obtain a handle to the read journal use the following:
```scala
val readJournal = PersistenceQuery(<actor system>).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
```
Similarly there is a JavaDsl version.

* There is a new experimental module in akka called `akka-persistence-query-experimental`.
  * This provides a way to create an `akka-streams Source` of data from a query posed to the journal plugin
  * The implementation of queries are purely up to the journal developer
  * This is a very fluid part of `akka-persistence` for the moment, so expect it to be quite unstable
* Initially three queries are supported.  All are `current` only for the moment, and thus would complete when no more records are available:
1. `currentPersistenceIds` (akka standard) - Provides a `Source[String,Unit]` of all of the persistence ids in the journal currently.  The results will be sorted by `persistenceId`.
1. `currentEventsByPersistenceId` (akka standard) - Provides a `Source[EventEnvelope,Unit]` of events matching the query.  This can be used to mimic recovery, for example replacing a deprecated `PersistentView` with another actor.
1. `AllEvents` (driver specific) - Provides a `Source[EventEnvelope,Unit]` of every event in the journal.  The results will be sorted by `persistenceId` and `sequenceNumber`.
* Eventually i'd like to support live versions of these queries, probably via a tailable cursor. (Tracked in issue #38)
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

<a name="mongouri"/>
##### Mongo URI

A mongo uri can be specified.  This must meet the requirements of [Mongo's uri format](http://docs.mongodb.org/manual/reference/connection-string/).

For example `mongodb://user:secret@localhost:27017/my-akka-persistence`.  If the `database name` is unspecified, it will be defaulted to `akka-persistence`.

```
akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://user:password@192.168.0.1:27017,192.168.0.2:27017/replicated-database"
```

If a user, password, and database are specified, the database will be used both as a credentials source as well as journal and/or snapshot storage.  
In order to use a separate database for data storage, one can provide this with the following configuration item:
   
```
akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://user:password@localhost/credential-database"
akka.contrib.persistence.mongodb.mongo.database = "storage-db"
```

Proper MongoDB user permissions must be in place of course for the user to be able to access `storage-db` in this case 

<a name="mongocollection"/>
##### Mongo Collection, Index settings

A DB name can be specified, as can the names of the collections and indices used (one for journal, one for snapshots).

```
akka.contrib.persistence.mongodb.mongo.journal-collection = "my_persistent_journal"
akka.contrib.persistence.mongodb.mongo.journal-index = "my_journal_index"
akka.contrib.persistence.mongodb.mongo.snaps-collection = "my_persistent_snapshots"
akka.contrib.persistence.mongodb.mongo.snaps-index = "my_snaps_index"
akka.contrib.persistence.mongodb.mongo.journal-write-concern = "Acknowledged"
```

<a name="writeconcern"/>
##### Mongo Write Concern settings

This is well described in the [MongoDB Write Concern Documentation](http://docs.mongodb.org/manual/core/write-concern/)

The write concern can be set both for the journal plugin as well as the snapshot plugin.  Every level of write concern is supported.  The possible concerns are listed below in decreasing safety:

 * `ReplicaAcknowledged` - requires a replica to acknowledge the write, this confirms that at least two servers have seen the write
 * `Journaled` <DEFAULT> - requires that the change be journaled on the server that was written to.  Other replicas may not see this write on a network partition
 * `Acknowledged` - also known as "Safe", requires that the MongoDB server acknowledges the write.  This does not require that the change be persistent anywhere but memory.
 * `Unacknowledged` - does not require the MongoDB server to acknowledge the write.  It may raise an error in cases of network issues.  This was the default setting that MongoDB caught a lot of flak for, as it masked errors in exchange for straight-line speed.  This is no longer a default in the driver.
 * ~~`ErrorsIgnored` - !WARNING! Extremely unsafe.  This level may not be able to detect if the MongoDB server is even running.  It will also not detect errors such as key collisions.  This makes data loss likely.  In general, don't use this.~~
 * Errors ignored is no longer supported as a write concern.

It is a bad idea&trade; to use anything less safe than `Acknowledged` on the journal.  The snapshots can be played a bit more fast and loose, but the recommendation is not to go below `Acknowledged` for any serious work.

As a single data point (complete with grain of salt!) on a MBP i5 w/ SSD with [Kodemaniak's testbed](https://github.com/kodemaniak/akka-persistence-throughput-test) and mongodb running on the same physical machine, the performance difference between:
 * `Journaled` and `Acknowledged` write concerns is two orders of magnitude
 * `Acknowledged` and `Unacknowledged` write concerns is one order of magnitude

`Journaled` is a significant trade off of straight line performance for safety, and should be benchmarked vs. `Acknowledged` for your specific use case and treated as an engineering cost-benefit decision.  The argument for the unsafe pair of `Unacknowledged` vs. `Acknowledged` seems to be much weaker, although the functionality is left for the user to apply supersonic lead projectiles to their phalanges as necessary :).

In addition to the mode of write concern, the `wtimeout` and `fsync` parameters may be configured seperately for the journal and snapshot.  FSync cannot be used with Journaling, so it will be disabled in that case.

Default values below:
```
akka.contrib.persistence.mongodb.mongo.journal-wtimeout = 3s
akka.contrib.persistence.mongodb.mongo.journal-fsync = false
akka.contrib.persistence.mongodb.mongo.snaps-wtimeout = 3s
akka.contrib.persistence.mongodb.mongo.snaps-fsync = false
```

<a name="circuitbreaker"/>
##### Circuit breaker settings

By default the circuit breaker is set up with a `maxTries` of 5, and `callTimeout` and `resetTimeout` of 5s.  [Akka's circuit breaker documentation](http://doc.akka.io/docs/akka/snapshot/common/circuitbreaker.html) covers in detail what these settings are used for.  In the context of this plugin, you can set these in the following way:

```
akka.contrib.persistence.mongodb.mongo.breaker.maxTries = 3
akka.contrib.persistence.mongodb.mongo.breaker.timeout.call = 3s
akka.contrib.persistence.mongodb.mongo.breaker.timeout.reset = 10s
```

These settings may need tuning depending on your particular environment.  If you are seeing `CircuitBreakerOpenException`s in the log without other errors that can mean that timeouts are occurring.  Make sure to look at the mongo slow logs as part of the research.  Here are some prior tickets where these issues have been discussed in the past (including an explanation of how to disable the Circuit Breaker):

[1](https://github.com/scullxbones/akka-persistence-mongo/issues/24)
[2](https://github.com/scullxbones/akka-persistence-mongo/issues/22)

Make sure to look into the [Backoff Supervisor](http://doc.akka.io/docs/akka/snapshot/scala/persistence.html#Failures).  Also, `onPersistRejected` can be caught and examined.  Between these two components, it should be possible to manage backpressure from MongoDB communicated via `CircuitBreaker`. 

<a name="dispatcher"/>
##### Configuring the dispatcher used

The name `akka-contrib-persistence-dispatcher` is mapped to a typically configured `ThreadPoolExecutor` based dispatcher.  This is needed to support the `Future`s used to interact with MongoDB via Casbah.  More details on these settings can be found in the [Akka Dispatcher documentation](http://doc.akka.io/docs/akka/snapshot/scala/dispatchers.html).  For example the (by core-scaled) pool sizes can be set:

```
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-min = 10
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-factor = 10
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-max = 20
```


<a name="passthru"/>
##### Passing DB objects directly into journal collection

If you need to see contents of your events directly in database in non-binary form, you can call `persist()` with `DBObject` (using casbah driver) or `BSONDocument` (using reactivemongo).

```scala
case class Command(value: String)
case class SampleState(counter: Int, lastValue: Option[String])

class SampleActor extends PersistentActor {
  
  var state = SampleState(0,None)

  def updateState(event: DBObject): Unit = {
    state = state.copy(counter = state.counter + 1, lastValue = event.getAs[String]("value"))
  }

  val receiveCommand: Receive = {
    case Command(value) =>
      persist(DBObject("value" -> value))(updateState)
  }

  // receiveRecover implementation, etc rest of class 
}

```

During replay, events will be sent to your actor as-is. It is the application's duty to handle BSON (de)serialization in this case.

This functionality is also exposed for snapshots.

<a name="legacyser"/>
##### Legacy Serialization

Legacy serialization (0.x) can be forced via the configuration parameter:

```
akka.contrib.persistence.mongodb.mongo.use-legacy-serialization = true
```

This will fully delegate serialization to `akka-serialization` by directly persisting the `PersistentRepr` as binary.  It can be used to carry over functionality that is dependent on the way that the `0.x` series used to treat storage of events.

<a name="metrics"/>
##### Metrics (optional functionality)

Depends on the excellent [Metrics-Scala library](https://github.com/erikvanoosten/metrics-scala) which in turn stands on the shoulders of codahale's excellent [Metrics library](https://github.com/dropwizard/metrics).

For this implementation, no assumptions are made about how the results are reported.  Unfortunately this means you need to inject your own reporters.  This will require you to refer to the extension in your own code, e.g.:

```scala

object MyApplication {

  val actorSystem = ActorSystem("my-application")
  val registry = MongoPersistenceExtension(actorSystem).registry
  val jmxReporter = JmxReporter.forRegistry(registry).build()
  jmxReporter.start()

}

```

##### What is measured?
Timers:
 - Journal append
 - Journal range delete
 - Journal replays
 - Journal max sequence number for processor query

Histograms:
 - Batch sizes used for appends

#### Future plans?
 - Adding metrics to snapshotter
 - Adding health checks to both

<a name="multiplugin"/>
##### Multiple plugin configurations

With the introduction of the `journalPluginId` and `snapshotPluginId` parameters as documented [here](http://doc.akka.io/docs/akka/2.4.0/scala/persistence.html#Multiple_persistence_plugin_configurations),
individual `PersistentActor`s can select a particular plugin implementation.

This plugin supports multiple instances with different configurations.  One use case may be pointing different actors at different databases.  To specify
multiple instances of this plugin, something like the following can be added to the `application.conf`:

```
# Supply default uri
akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://defaultHost:27017/db1"

akka-contrib-mongodb-persistence-journal-other {
  # Select this plugin as journal implementation
  class = "akka.contrib.persistence.mongodb.MongoJournal"
  # Use delivered dispatcher
  plugin-dispatcher = "akka-contrib-persistence-dispatcher"
  # Overrides to supply overridden parameters (can be anything) 
  # - assumed config root is `akka.contrib.persistence.mongodb.mongo`
  overrides = {
    mongouri = "mongodb://host1:27017/special"
  }
}
```

Given the above configuration, all `PersistentActor`s will default to the "defaultHost db1" pair.  

In addition, some can specify `journalPluginId = "akka-contrib-mongodb-persistence-journal-other" and use the "host1 special" pair.

Some more information is covered in [#43](https://github.com/scullxbones/akka-persistence-mongo/issues/43)
