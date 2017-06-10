## A MongoDB plugin for [akka-persistence](http://akka.io)

 * Three projects, a core and two driver implementations.  To use you must pull jars for *only one* of the drivers. Common will be pulled in as a transitive dependency:
   * common provides integration with Akka persistence, implementing the plugin API
   * casbah provides an implementation against the casbah driver
   * rxmongo provides an implementation against the ReactiveMongo driver
 * The tests expect two mongods running, with and without authentication.  A utility script will boot these as docker containers.
   * A `CONTAINER_HOST` environment variable must be set with the docker host endpoint. The default is `localhost`
     * If using `docker-machine`, `export CONTAINER_HOST=$(docker-machine ip default)` should set the variable correctly for the machine named "default"
     * If using `dlite`, `export CONTAINER_HOST=docker.local` should set the variable correctly
 * Supports Akka 2.4 series
 * Supports MongoDB major versions 2.6, 3.0, 3.2
 * Cross-compiled against scala `2.11` and `2.12`
 * Be aware that there is a `16MB` payload size limit on snapshots and journal events.  In addition a journal batch must be <= `16MB` in size.  A journal batch is defined by the `Seq` of events passed to `persistAll`.

### Change log is [here](changelog24.md)

### Quick Start

* Choose a driver - Casbah and ReactiveMongo are currently supported
* Driver dependencies are `provided`, meaning they must be included in the application project's dependencies.
* Add the following to sbt:

(Casbah)
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-casbah" % "1.4.3"
```
(Reactive Mongo)
##### Please note: Supported versions of reactive mongo require the `0.12` series, with a minimum version number of `0.12.0`
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-rxmongo" % "1.4.3"
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
   * [ReactiveMongo Failover](#rxmfailover)
   * [Dispatcher](#dispatcher)
   * [Pass-Through BSON](#passthru)
   * [Legacy Serialization](#legacyser)
   * [Metrics](#metrics)
   * [Multiple plugins](#multiplugin)
   * [Casbah Client Settings](#casbahsettings)
1. [Suffixed collection names](#suffixcollection)
   * [Overview](#suffixoverview)
   * [Usage](#suffixusage)
   * [Details](#suffixdetail)
   * [Migration tool](#suffixmigration)

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
* All `current` and `realtime` queries are supported, except eventsByTag.
1. `currentPersistenceIds` (akka standard) - Provides a `Source[String,Unit]` of all of the persistence ids in the journal currently.  The results will be sorted by `persistenceId`.
1. `allPersistenceIds` (akka standard) - Provides a `Source[String, Unit]` of all persistence ids in the journal. The Source not completes when reaches end of events and continues emit as new persistence ids are persist into journal
1. `currentEventsByPersistenceId` (akka standard) - Provides a `Source[EventEnvelope,Unit]` of events matching the query.  This can be used to mimic recovery, for example replacing a deprecated `PersistentView` with another actor.
1. `eventsByPersistenceId` (akka standard) - Provides a `Source[EventEnvelope,Unit]` of events matching the query. The Source not completes when reaches end of events and continues emit as new events are persist into journal
1. `currentAllEvents` (driver specific) - Provides a `Source[EventEnvelope,Unit]` of every event in the journal.  The results will be sorted by `persistenceId` and `sequenceNumber`.
1. `allEvents` (driver specific) - Provides a live version of currentAllEvents
* I'll look for community feedback about what driver-specific queries might be useful as well
* The live queries use capped collection to stream events. If you do not use live queries you can disable the inserts into capped collection with `akka.contrib.persistence.mongodb.mongo.realtime-enable-persistence = false`

<a name="miscchanges"/>
#### Miscellaneous Other Changes

* The `CircuitBreaker` implementation operates a little differently:
  * Writes operate as before
  * Reads (think replays) do not contribute to timeouts or errors, but if the `CircuitBreaker` is in `Open` state, replays will fail fast.
* Travis now verifies builds and tests run against all supported Mongo versions as well as Scala cross compile versions
* Several metrics were no longer relevant due to journal changes in 2.4

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

<a name="rxmfailover"/>
##### Reactive Mongo Driver - failover settings

The reactive mongo driver supports specific failover settings which govern behavior when sending requests results in failures.

* `initialDelay` - this duration is the delay used between the first error and the first retry
* `retries` - this is the number of retries attempted before giving up
* `growth` - this describes the growth function, 3 choices are available:
  * `con` - constant delay equal to `factor`
  * `lin` - linear increasing delay equal to the current retry number.  Equivalent to `exp` with `factor` = 1
  * `exp` - exponentially increasing delay to the power of `factor`
* `factor` - used in conjunction with `growth` as described above

```
akka.contrib.persistence.mongodb.rxmongo.failover {
    initialDelay = 50ms
    retries = 3
    growth = exp
    factor = 1.5
}
```

See [Reactive Mongo documentation](http://reactivemongo.org/releases/0.12/documentation/advanced-topics/failoverstrategy.html) for more information.

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
  # use this parametes to config akka persistence
  circuit-breaker {
    max-failures = 10
    call-timeout = 10s
    reset-timeout = 30s
  }
  replay-filter {
    mode = repair-by-discard-old
    window-size = 100
    max-old-writers = 10
    debug = on
  }
}

akka-contrib-mongodb-persistence-journal-other-snapshot {
  # Select this plugin as journal snapshot implementation
  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
  # Overrides to supply overridden parameters (can be anything) - config root is `akka.contrib.persistence.mongodb.mongo`
  overrides = {
    mongouri = "mongodb://host1:27017/special"
  }  
  # use this parametes to config akka persistence
  circuit-breaker {
    max-failures = 10
    call-timeout = 10s
    reset-timeout = 30s
  }
  replay-filter {
    mode = repair-by-discard-old
    window-size = 100
    max-old-writers = 10
    debug = on
  }
}

```

Given the above configuration, all `PersistentActor`s will default to the "defaultHost db1" pair.  

In addition, some can specify `journalPluginId = "akka-contrib-mongodb-persistence-journal-other" and use the "host1 special" pair.

Some more information is covered in [#43](https://github.com/scullxbones/akka-persistence-mongo/issues/43)

<a name="casbahsettings"/>
##### Casbah Client Settings
The Java MongoDB Driver, Casbah is based on, supports various connection related settings, which can be overriden in
`application.conf`:

```
akka.contrib.persistence.mongodb.casbah{
  minpoolsize = 0
  maxpoolsize = 100
  waitqueuemultiple = 5
  serverselectiontimeout = 30seconds
  waitqueuetimeout = 2minutes
  maxidletime = 0
  maxlifetime = 0
  connecttimeout = 10seconds
  sockettimeout = 0seconds
  socketkeepalive = false
  ssl = false
  sslinvalidhostnameallowed = false
  heartbeatfrequency = 10seconds
  minheartbeatfrequency = 500ms
  heartbeatconnecttimeout = 20seconds
  heartbeatsockettimeout = 20seconds
}
```

Be aware that some of them can be set via the MongoURI as well, in that case settings from MongoURI override the explicit
settings.

<a name="suffixcollection"/>
### Suffixed collection names

<a name="suffixoverview"/>
#### Overview
Without any further configuration, events are stored in some unique collection, named by default "akka_persistence_journal", while snapshots are stored in "akka_persistence_snaps". This is the primary and widely used behavior of event sourcing through Akka-persistence, but it may happen to be insufficient in some cases.

As described in issue [#39](https://github.com/scullxbones/akka-persistence-mongo/issues/39), some kind of `persistenceId` mapping to collection names should do the trick, and this is what inspired the *suffixed collection names* feature development.

The main idea here is to create as many journal and snapshot collections as needed, which names are built from default (or [configured](#mongocollection)) names, *suffixed* by a separator, followed by some information "picked" from `persistenceId`.

Additionally, we provide a trait called `CanSuffixCollectionNames` that should be extended / mixed in some class, leading to override:
* a `getSuffixfromPersistenceId` function allowing to "pick" relevant information from `persistenceId`
* a `validateMongoCharacters` function allowing to replace any [MongoDB forbidden character](https://docs.mongodb.com/manual/reference/limits/#naming-restrictions) (including the separator)

```scala
def getSuffixfromPersistenceId(persistenceId: String): String
def validateMongoCharacters(input: String): String
```

For example, say that:
* `persistenceId` is "suffix-test" for some `PersistentActor`
* separator is the underscore character
* `getSuffixfromPersistenceId` removes the string "-test" from any string passed in argument

journal name would be "akka_persistence_journal_*suffix*" while snapshot name would be "akka_persistence_snaps_*suffix*"

##### Important note:
Capped collections keep their name, respectively "akka_persistence_realtime" and "akka_persistence_metadata" by default. They remain out of *suffixed collection names* feature scope.

<a name="suffixusage"/>
#### Usage
Using the *suffixed collection names* feature is a matter of configuration and a little code writing.

##### Configuration
Inside your `application.conf` file, use the following lines to enable the feature:
```
akka.contrib.persistence.mongodb.mongo.suffix-builder.separator = "_"
akka.contrib.persistence.mongodb.mongo.suffix-builder.class = "com.mycompany.myproject.SuffixCollectionNames"
```

Nothing happens as long as you do not provide a class extending or mixing in `akka.contrib.persistence.mongodb.CanSuffixCollectionNames` trait, nor if its `getSuffixfromPersistenceId` method **always** returns an empty string.

First line defines a separator as a `String`, but only its first character will be used as a separator (keep in mind that mongoDB collection names are limited in size) By default, this property is set to an underscore character "_".

Second line contains the entire package+name of the user class extending or mixing in `akka.contrib.persistence.mongodb.CanSuffixCollectionNames` trait (see below).

Optionally, you can choose to drop empty suffixed collections once they are empty, in order, for example, not to fill your database with a great number of useless collections. Be aware, however, that the underlying process is built upon MongoDB **non atomic** `count` and `drop` operations that *may* lead to race conditions. To enable this feature, just add the following line to your `application.conf` file:
```
akka.contrib.persistence.mongodb.mongo.suffix-drop-empty-collections = true
```

##### Code
Add some `com.mycompany.myproject.SuffixCollectionNames` class in your code, extending or mixing in `akka.contrib.persistence.mongodb.CanSuffixCollectionNames` trait:

```scala
package com.mycompany.myproject

import akka.contrib.persistence.mongodb.CanSuffixCollectionNames

class SuffixCollectionNames extends CanSuffixCollectionNames {

    override def getSuffixfromPersistenceId(persistenceId: String): String = persistenceId match {
      // in this example, we remove any leading "-test" string from persistenceId passed as parameter
      case str: String if (str.endsWith("-test")) => str.substring(0, str.indexOf("-test"))
      // otherwise, we do not suffix our collection
      case _ => ""
    }

  override def validateMongoCharacters(input: String): String = {
    // According to mongoDB documentation,
    // forbidden characters in mongoDB collection names (Unix) are /\. "$
    // Forbidden characters in mongoDB collection names (Windows) are /\. "$*<>:|?
    // in this example, we replace each forbidden character with an underscore character   
    val forbidden = List('/', '\\', '.', ' ', '\"', '$', '*', '<', '>', ':', '|', '?')

    input.map { c => if (forbidden.contains(c)) '_' else c }
  }
}
```

Remember that **always** returning an empty `String` will *not* suffix any collection name, even if some separator is defined in the configuration file.

##### Important note:
Keep in mind, while designing `getSuffixfromPersistenceId` and `validateMongoCharacters` methods, that there are [limitations regarding collection names in MongoDB](https://docs.mongodb.com/manual/reference/limits/#naming-restrictions). It is the responsability of the developer to ensure that his `getSuffixfromPersistenceId` and `validateMongoCharacters` methods take these constraints into account.

**Pay particularly attention to collection and index name length**. For example, with default database, journals, snapshots and their respective indexes names, your suffix, obtained through `getSuffixfromPersistenceId` and `validateMongoCharacters` methods, should not exceed 53 characters long.

<a name="suffixdetail"/>
#### Details

##### Batch writing
Writes remain *atomic at the batch level*, as explained [above](#model) but, as events are now persisted in a "per collection manner", it does not mean anymore that *if the plugin is sent 100 events, these are persisted in mongo as a single document*.

Events are first *grouped* by collection, then batch-persisted, each group of events in its own correspondant suffixed journal. This means our 100 events may be persisted in mongo as *several* documents, decreasing performances but allowing multiple journals.

If enabled (via the `akka.contrib.persistence.mongodb.mongo.realtime-enable-persistence` configuration property) inserts inside capped collections for live queries are performed the usual way, in one step. No grouping here, our 100 events are still persisted as a single document in "akka_persistence_realtime" collection.

##### Reading
Instead of reading a single journal, we now collect all journals and, for each of them, perform the appropriate Mongo queries.

Of course, for reading via the "xxxByPersistenceId" methods, we directly point to the correspondant journal collection.

<a name="suffixmigration"/>
#### Migration tool

##### Overview
We provide a **basic** migration tool from **1.x** unique journal and snapshot to *suffixed collection names*. Unless the [migration of 0.x journal](#migration), this process cannot be performed on the fly, as it directly deals with (and builds) collections inside the database. So, yes, you have to stop your application during the migration process...

###### How does it work ?
The main idea is to parse unique journal, pick up every record, insert it in newly created appropriate suffixed journal, and finally remove it from unique journal. Additionally, we do the same for snapshots, and remove all records from "akka_persistence_metadata" capped collection. This capped collection will be built again through usual event sourcing process...

Of course, this process would be very long, but thanks to *aggregation*, we actually "gather" records by future suffixed collection, append them **one by one** to that new suffixed collection, and remove them **in one step**, from unique original collection. Appending records to new collection *one by one* may appear as a bad choice regarding performance issues, but trying to append a great number of records in a single bulk operation may lead to `OutOfMemoryError` exception. Remember, its a *basic* tool and there is no need to hurry here, as this is actually a maintenance operation. So, once more, let's keep it simple but efficient.

###### Recommended migration steps:
* **backup your database** (use, for example, the `mongodump` command)
* stop your application
* update your configuration to prepare for migration (see below)
* run the migration tool (this may take a while)
* update your configuration again to remove migration process but keep *suffixed collection names* feature
* start your application

Optionally, you could perform the entire process on some dummy database and application running offline, for example to determine how long it takes.

Remember that you work **directly** inside the database, so do not forget about replication if you have several mongo servers...

##### Usage
From now on, we refer to unique journal as "akka_persistence_journal" and unique snapshot as "akka_persistence_snapshot" even if they could have different names through [configuration](#mongocollection) (let's keep those explanations simple)

First of all, **backup your database and stop your application**.

Using the *suffixed collection names* migration tool is a matter of configuration and a little code writing, and the first thing you should do is enable the *suffixed collection names* feature as explained in [*suffixed collection names* usage](#suffixusage). From now on, we consider that you have provided appropriate properties in your `application.conf` file and written your `getSuffixfromPersistenceId` and `validateMongoCharacters` methods that do not **always** return an empty string (if they do, nothing will be migrated)

###### Important note
Design your `getSuffixfromPersistenceId` and `validateMongoCharacters` methods **carefully**, as this migration process **does not work** from suffixed collections depending on some `getSuffixfromPersistenceId` and `validateMongoCharacters` methods to *new* suffixed collections depending on some *modified* `getSuffixfromPersistenceId` and `validateMongoCharacters` methods !

Of course, once this is done, you should **not** start your application, unless you want to run some tests on some dummy database !

###### Configuration
Add the following to your `build.sbt` file:
```scala
libraryDependencies ++= Seq( "com.github.scullxbones" %% "akka-persistence-mongo-tools" % "1.4.3",
                             "org.mongodb" %% "casbah" % "3.1.0" )
```

Notice that even if you currently don't use it, migration process is performed through Casbah driver.

Notice that if you use Casbah driver, `"org.mongodb" %% "casbah" % "3.1.0"` dependency should already be part of your `build.sbt` file.

Additionally, you may configure your logging system with **INFO** level for `MigrateToSuffixedCollections` class, otherwise there will be no output to console or log files. With *log4J*, this should be done like that:
```xml
<logger name="akka.contrib.persistence.mongodb.MigrateToSuffixedCollections" level="INFO" />
```

###### Code
Provide an `ActorSystem`, instantiate a `MigrateToSuffixedCollections` class and call its `migrateToSuffixCollections` method as shown in the following example:
```scala
package com.mycompany.myproject.myapplication.main

object Migrate extends App {
    import akka.actor.ActorSystem
    val system: ActorSystem = ActorSystem("my system name", myConfig)

    import akka.contrib.persistence.mongodb.MigrateToSuffixedCollections
    val migration = new MigrateToSuffixedCollections(system)
    try {
        migration.migrateToSuffixCollections()
    } catch {
        case t: Throwable =>
            println("Error occurred on migration to suffixed collections")
            t.printStackTrace()
            System.exit(-1)
    }
}
```
Providing an `ActorSystem` depends on the manner your application is designed and is beyond the scope of this documentation.

As the process **must** be performed offline, its a good idea to use an object (we call it "Migrate" in our example) extending the `scala.App` trait and run it through the `sbt run` command that allows us to choose which one to run:

```
Multiple main classes detected, select one to run:

 [1] com.mycompany.myproject.myapplication.main.Main
 [2] com.mycompany.myproject.myapplication.main.Migrate

Enter number:
```
If we choose number 2 here, we should see something like this (remember to configure INFO level for `MigrateToSuffixedCollections` class)
```
2016-09-23_15:43:31.823  INFO - Starting automatic migration to collections with suffixed names
This may take a while...
2016-09-23_15:43:36.519  INFO - 1/1 records were inserted into 'akka_persistence_journal_foo1'
2016-09-23_15:43:36.536  INFO - 1/1 records, previously copied to 'akka_persistence_journal_foo1', were removed from 'akka_persistence_journal'
2016-09-23_15:43:36.649  INFO - 24/24 records were inserted into 'akka_persistence_journal_foo2'
2016-09-23_15:43:36.652  INFO - 24/24 records, previously copied to 'akka_persistence_journal_foo2', were removed from 'akka_persistence_journal'
2016-09-23_15:44:58.090  INFO - 74013/74013 records were inserted into 'akka_persistence_journal_foo3'
2016-09-23_15:45:07.559  INFO - 74013/74013 records, previously copied to 'akka_persistence_journal_foo3', were removed from 'akka_persistence_journal'
2016-09-23_15:45:20.423  INFO - 54845/54845 records were inserted into 'akka_persistence_journal_foo4'
2016-09-23_15:45:25.494  INFO - 54845/54845 records, previously copied to 'akka_persistence_journal_foo4', were removed from 'akka_persistence_journal'
2016-09-23_15:45:25.502  INFO - 76 records were ignored and remain in 'akka_persistence_journal'
2016-09-23_15:45:25.502  INFO - JOURNALS: 128883/128959 records were successfully transfered to suffixed collections
2016-09-23_15:45:25.502  INFO - JOURNALS: 76/128959 records were ignored and remain in 'akka_persistence_journal'
2016-09-23_15:45:25.502  INFO - JOURNALS: 128883 + 76 = 128959, all records were successfully handled
2016-09-23_15:45:25.785  INFO - 2/2 records were inserted into 'akka_persistence_snaps_foo4'
2016-09-23_15:45:25.788  INFO - 2/2 records, previously copied to 'akka_persistence_snaps_foo4', were removed from 'akka_persistence_snaps'
2016-09-23_15:45:25.915  INFO - 101/101 records were inserted into 'akka_persistence_snaps_foo3'
2016-09-23_15:45:25.931  INFO - 101/101 records, previously copied to 'akka_persistence_snaps_foo3', were removed from 'akka_persistence_snaps'
2016-09-23_15:45:25.932  INFO - SNAPSHOTS: 103/103 records were successfully transfered to suffixed collections
2016-09-23_15:45:25.936  INFO - METADATA: 106/106 records were successfully removed from akka_persistence_metadata collection
2016-09-23_15:45:25.974  INFO - Automatic migration to collections with suffixed names has completed
```
Notice that records **may** remain in unique collections "akka_persistence_journal" and "akka_persistence_snapshot" in case your `getSuffixfromPersistenceId` and `validateMongoCharacters` methods sometimes return an empty string. In that case, an information regarding these records is printed in the console above, and a warning is also printed if *migrated* + *ignored* records does not equal *total* records.

Notice that unique collections "akka_persistence_journal" and "akka_persistence_snapshot" remain in the database, even if empty. You should remove them if you want, using mongo shell...

###### What's next ?
**Keep *suffixed collection names* feature enabled** as explained in [*suffixed collection names* usage](#suffixusage), and of course, **do not modify** your `getSuffixfromPersistenceId` and `validateMongoCharacters` methods.

Keep your database safe, **avoid running again the migration process**, so:
* remove migration code (in our example, we remove our `Migrate` object)
* remove `akka-persistence-mongo-tools` dependency from your `build.sbt` file

That's it, you should **start your application** and enjoy *suffixed collection names* feature.
