## A MongoDB plugin for [akka-persistence](http://akka.io)

 * Three projects, a core and two driver implementations.  To use you must pull jars for *only one* of the drivers. Common will be pulled in as a transitive dependency:
   * common provides integration with Akka persistence, implementing the plugin API
   * rxmongo provides an implementation against the ReactiveMongo driver
   * scala provides an implementation against the MongoDB official driver
 * The tests expect two mongods running, with and without authentication.  A utility script (`test_containers.sh`) will boot these as docker containers.
 * Supports Akka 2.6 series
 * Test suite runs against MongoDB major versions 3.6, 4.0, 4.2
 * Cross-compiled against scala `2.12` and `2.13`
 * Be aware that there is a `16MB` payload size limit on snapshots and journal events.  In addition a journal batch must be <= `16MB` in size.  A journal batch is defined by the `Seq` of events passed to `persistAll`.

### Change log is [here](changelog26.md)

### Quick Start

* Choose a driver - MongoDB official Scala and ReactiveMongo are currently supported
* Add the following to sbt:

(Official Scala)
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-scala" % "3.0.6"
```

(Reactive Mongo)
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-rxmongo" % "3.0.6"
```
* Inside of your `application.conf` file, add the following line if you want to use the journal (snapshot is optional).  The driver selection should be pulled in by a `reference.conf` in the driver jar you choose:
```
akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
```

### Details
1. [Major changes in 3.x](#major)
1. [Configuration Details](#config)
   * [Mongo URI](#mongouri)
   * [Collection and Index](#mongocollection)
   * [Collection Caches](#collectioncaches)
   * [Write Concerns](#writeconcern)
   * [ReactiveMongo Failover](#rxmfailover)
   * [Stream buffer size](#buffer-size)
   * [Dispatcher](#dispatcher)
   * [Pass-Through BSON](#passthru)
   * [Legacy Serialization](#legacyser)
   * [Metrics](#metrics)
   * [Multiple plugins](#multiplugin)
   * [Casbah/Official Scala Client Settings](#officialsettings)
1. [Suffixed collection names](#suffixcollection)
   * [Overview](#suffixoverview)
   * [Usage](#suffixusage)
   * [Details](#suffixdetail)
   * [Migration tool](#suffixmigration)

<a name="major"/>

### Major Changes in 3.x

<a name="akka26"/>

#### Akka 2.6 support
* The driving change for this new major version of the library is support of Akka 2.6
* More information in [the migration guide](https://doc.akka.io/docs/akka/2.6/project/migration-guide-2.5.x-2.6.x.html)

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

Proper MongoDB user permissions must be in place for the user to be able to access `storage-db` in this case

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

<a name="collectioncaches"/>

##### Collection Caches

The persistence plugin caches all collections it creates. As long as a collection remains in cache, the plugin will not re-create or re-index it for each access, which would cause unnecessary write-locks. By default, collection caches keep their entries forever and have unbounded memory consumption. It is best to implement your own collection cache if you enable [suffixed collection names](#suffixcollection).

Collection cache implementations should extend the trait `MongoCollectionCache` and have a public constructor receiving a `Config` object as argument. There are separate configurations for caches of journal, snapshot, realtime and metadata collections.

Default caches keep collections no longer than the duration specified in the configuration `expire-after-write`; setting it to a finite duration ensures that collections are eventually created with the correct options and indexes. Setting `max-size=1` restricts the number of cached collections to 1; this is useful for realtime and metadata collections or when [suffixed collection names](#suffixcollection) is disabled.

```
akka.contrib.persistence.mongodb.mongo {
  # Caches of collections created by the plugin
  collection-cache {

    # Cache of journal collections
    journal {
      # Implementation of the cache.
      # - Must be a subtype of MongoCollectionCache.
      # - Must have a public constructor taking a Config object as argument.
      # - Must be able to store the collection type of the chosen driver.
      #
      # If left empty, a default naive implementation with unbound memory consumption is used.
      class = ""

      # How long to retain the collection. Invalid or missing durations are treated as eternity.
      expire-after-write = Infinity
    }

    # Cache of snapshot collections
    snaps {
      class = ""
      expire-after-write = Infinity
    }

    # Cache of one realtime collection
    realtime {
      class = ""
      expire-after-write = Infinity

      # maximum size of the cache
      # 1 because the realtime collection is unique
      # default caches do not honor size bounds bigger than 1
      max-size = 1
    }

    # Cache of one metadata collection
    metadata {
      class = ""
      expire-after-write = Infinity

      # maximum size of the cache
      # 1 because the metadata collection is unique
      # default caches do not honor size bounds bigger than 1
      max-size = 1
    }
  }
}
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

<a name="buffer-size"/>

##### Configuring size of buffer for read stream

This functionality has effectively been removed with the change of live streams to open a cursor on the realtime collection with every read query.

The `.dropTail` behavior addressed by [#192](https://github.com/scullxbones/akka-persistence-mongo/pull/192) is no longer present.

~~`akka-persistence-mongo` uses streams to feed the read side with events. By default, the buffer's size is fixed at `1000`
To modify the default value for a specific use case, you can add the following configuration lines in your `application.conf`.~~

```  
akka.contrib.persistence.stream-buffer-max-size.stream-buffer-max-size.event-by-pid = [your value]
akka.contrib.persistence.stream-buffer-max-size.stream-buffer-max-size.all-events = [your value]
akka.contrib.persistence.stream-buffer-max-size.stream-buffer-max-size.events-by-tag = [your value]
akka.contrib.persistence.stream-buffer-max-size.stream-buffer-max-size.pid = [your value]
```

<a name="dispatcher"/>

##### Configuring the dispatcher used

The name `akka-contrib-persistence-dispatcher` is mapped to a typically configured `ThreadPoolExecutor` based dispatcher.  More details on these settings can be found in the [Akka Dispatcher documentation](http://doc.akka.io/docs/akka/snapshot/scala/dispatchers.html).  For example the (by core-scaled) pool sizes can be set:

```
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-min = 10
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-factor = 10
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-max = 20
```


<a name="passthru"/>

##### Passing DB objects directly into journal collection

If you need to see contents of your events directly in database in non-binary form, you can call `persist()` with a specific type that corresponds to the driver you use:

* `org.bson.BsonValue` (using native scala driver)
* `com.mongodb.DBObject` (using casbah driver)
* `reactivemongo.bson.BSONDocument` (using reactivemongo).

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

By default metrics depends on the excellent [Metrics-Scala library](https://github.com/erikvanoosten/metrics-scala) which in turn stands on the shoulders of codahale's excellent [Metrics library](https://github.com/dropwizard/metrics).

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

##### Use another Metrics Library
If you don't want to use the default metrics library, you can also provide your own implementation of
`akka.contrib.persistence.mongodb.MetricsBuilder` which will then be used to build your implementation of
`akka.contrib.persistence.mongodb.MongoTimer` and `akka.contrib.persistence.mongodb.MongoHistogram`.

To make akka-persistence-mongo use your `akka.contrib.persistence.mongodb.MetricsBuilder` implementation you need to
specify the property: `akka.contrib.persistence.mongodb.mongo.metrics-builder.class` with the full qualified class name
of your implementation of `akka.contrib.persistence.mongodb.MetricsBuilder`.

#### Future plans?
 - Adding metrics to snapshotter
 - Adding health checks to both

<a name="multiplugin"/>

##### Multiple plugin configurations

With the introduction of the `journalPluginId` and `snapshotPluginId` parameters as documented [here](https://doc.akka.io//docs/akka/2.6/persistence.html?language=scala#Multiple_persistence_plugin_configurations),
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

<a name="officialsettings"/>

##### Official Scala Client Settings
The Official MongoDB Drivers that are used support various connection related settings, which can be overriden via typesafe config e.g. `application.conf`:

(Official Scala)
```
akka.contrib.persistence.mongodb.official {
  minpoolsize = 0
  maxpoolsize = 100
  waitqueuemultiple = 5
  serverselectiontimeout = 30seconds
  waitqueuetimeout = 2minutes
  maxidletime = 0
  maxlifetime = 0
  connecttimeout = 10seconds
  sockettimeout = 0seconds
  ssl = false
  sslinvalidhostnameallowed = false
  heartbeatfrequency = 10seconds
  minheartbeatfrequency = 500ms
}
```

Any settings provided in the configured `mongouri` connection string will override explicit settings.

<a name="eventsbytag"/>

### EventsByTag queries
`EventsByTag` queries as described in [the akka docs](http://doc.akka.io/docs/akka/current/scala/persistence-query.html#eventsbytag-and-currenteventsbytag) are available for use.  When payloads are wrapped in an instance of `akka.persistence.journal.Tagged`, these tags are stored in the journal and can be used as part of a persistence query.  In DDD context, tags correspond well to aggregate roots.

An example query is below.  This is querying for all events tagged `foo`.  Alternately, an offset can be passed (stored for example from a previous query).  This offset is considered exclusive, so the query will restart at the next item (`>` rather than `>=` behavior).
```scala
    implicit val system = as
    implicit val am = ActorMaterializer()
    val readJournal = PersistenceQuery(system).readJournalFor[CurrentEventsByTagQuery](MongoReadJournal.Identifier)
    val result: Future[Seq[EventEnvelope]] = readJournal.currentEventsByTag("foo", Offset.noOffset).runWith(Sink.seq)
```

The mongo plugin understands either `Offset.noOffset` or an `ObjectId` offset.  Importing `akka.contrib.persistence.mongodb` will enhance the `Offset` companion with an extra method to produce an `ObjectId` offset generated from external means.

For offsets to operate correctly in a distributed environment, the system clocks of all journal-writing processes should be synchronized.  In addition, `ObjectId`s will be sorted by their components described in the [mongodb docs](https://docs.mongodb.com/manual/reference/method/ObjectId/).  This can cause events that are generated in the same second to be replayed out of strict temporal order.

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
Some collections keep their name, respectively "akka_persistence_realtime" and "akka_persistence_metadata" by default. They remain out of *suffixed collection names* feature scope.

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

Events are first *grouped* by collection, then batch-persisted, each group of events in its own correspondent suffixed journal. This means our 100 events may be persisted in mongo as *several* documents, decreasing performances but allowing multiple journals.

If enabled (via the `akka.contrib.persistence.mongodb.mongo.realtime-enable-persistence` configuration property) inserts inside capped collection for live queries are performed the usual way, in one step. No grouping here, our 100 events are still persisted as a single document in "akka_persistence_realtime" collection.

##### Reading
Instead of reading a single journal, we now collect all journals and, for each of them, perform the appropriate Mongo queries.

Of course, for reading via the "xxxByPersistenceId" methods, we directly point to the correspondent journal collection.

<a name="suffixmigration"/>

#### Migration tool

##### Overview
We provide a **basic** migration tool from **1.x** unique journal and snapshot to *suffixed collection names*. Unless the [migration of 0.x journal](#migration), this process cannot be performed on the fly, as it directly deals with (and builds) collections inside the database. So, yes, you have to stop your application during the migration process...

###### How does it work ?
The main idea is to parse unique journal, pick up every record, insert it in newly created appropriate suffixed journal, and finally remove it from unique journal. We do the same for snapshots, and optionally remove all records from "akka_persistence_metadata" collection, allowing it to be built again through usual event sourcing process...

Of course, this process would be very long, but thanks to *aggregation*, we actually "gather" records by future suffixed collection, then by *persistence Id*, append (i.e. *INSERT*) them **in one step** (meaning all records of each *persistence Id*) to that new suffixed collection, and remove (i.e. *DELETE*) them **in one step**, from unique original collection.

Additionally, we offer the possibility to try these *INSERT* and *DELETE* operations multiple times, as the process runs such operations in parallel and may lead to Mongo timeouts. We also offer the same possibility for removing all records from "akka_persistence_metadata" collection (see configuration below)

###### Heavy load
In case running operations in parallel leads to Mongo overload (for example errors like `com.mongodb.MongoWaitQueueFullException: Too many threads are already waiting for a connection`) we provide the ability to really perform the migration in a "*one at a time*" manner. We then process one *persistence Id* after the other, and for each *persistence Id*, append (i.e. *INSERT*) one record after the other and, if successful, remove (i.e. *DELETE*) it just after it has been transferred.

So, we first "gather" records by *persistence Id* but not by future suffixed collection. This process is much longer but Mongo should never been overwhelmed.

Additionally, we offer the possibility to run operations in parallel, that is to process a predetermined amount of *persistence Ids* at the same time. Once all these *persistence Ids* are processed (we actually wait for the slowest) we handle the following ones. This allows to decrease migration duration a little bit and is actually a matter of tuning (as an example, migrating 33 millions records for about 500 *persistence Ids* that were never snapshotted (yes, it was due to a bug) with a *parallelism* of 50, took approximately 7 hours on a PC with 4 CPUs and a RAM of 8 Mo...)

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
libraryDependencies += "com.github.scullxbones" %% "akka-persistence-mongo-tools" % "3.0.6"
```
Notice that even if you currently don't use it, migration process is performed through Official Scala driver.

Notice that if you use Official Scala driver, `"org.mongodb.scala" %% "mongo-scala-driver" % "..."` dependency should already be part of your `build.sbt` file.

Additionally, you may configure your logging system with **INFO** level for `ScalaDriverMigrateToSuffixedCollections` class, otherwise there will be no output to console or log files. With *log4J*, this should be done like that:
```xml
<logger name="akka.contrib.persistence.mongodb.ScalaDriverMigrateToSuffixedCollections" level="INFO" />
```

Optionally, you can configure, through the following properties:

* in case you choose the *normal* migration:

    how many times *INSERT* and *DELETE* operations may take place:
```
akka.contrib.persistence.mongodb.mongo.suffix-migration.max-insert-retry = 1
akka.contrib.persistence.mongodb.mongo.suffix-migration.max-delete-retry = 1
```
Careful, the value `0` means **unlimited** retries (not recommended)

* in case you choose the *heavy load* migration:

    how many *persistence Ids* may be processed at the same time:
```
akka.contrib.persistence.mongodb.mongo.suffix-migration.parallelism = 1
```


* in both cases:

    the ability to clear "akka_persistence_metadata" collection (defaults to false) and how many attempts may occur:
```
akka.contrib.persistence.mongodb.mongo.suffix-migration.empty-metadata = true
akka.contrib.persistence.mongodb.mongo.suffix-migration.max-empty-metadata-retry = 1
```
Careful, the value `0` means **unlimited** retries (not recommended)

Choosing among *normal* or *heavy load* migration is done via the following property:
```
akka.contrib.persistence.mongodb.mongo.suffix-migration.heavy-load = false
```

###### Code
Provide an `ActorSystem`, instantiate a `ScalaDriverMigrateToSuffixedCollections` class and call its `migrateToSuffixCollections` method as shown in the very basic following example:
```scala
package com.mycompany.myproject.myapplication.main


import akka.actor.ActorSystem
val system: ActorSystem = ActorSystem("my system name", myConfig)

import akka.contrib.persistence.mongodb.ScalaDriverMigrateToSuffixedCollections
try {
    Await.result(new ScalaDriverMigrateToSuffixedCollections(system).migrateToSuffixCollections, myDuration)
} catch {
    case t: Throwable =>
        println("Error occurred on migration to suffixed collections")
        t.printStackTrace()
}

```
Providing an `ActorSystem` depends on the manner your application is designed and is beyond the scope of this documentation.

Running this process, we should see something like this (remember to configure INFO level for `ScalaDriverMigrateToSuffixedCollections` class)

* in case you choose the *normal* migration:
```
2019-06-07  INFO - Starting automatic migration to collections with suffixed names
This may take a while...
2019-06-07  INFO -

JOURNALS: Gathering documents by suffixed collection names.  T h i s   m a y   t a k e   a   w h i l e  ! ! !   It may seem to freeze, be patient...

2019-06-07  INFO - Processing suffixed collection 'akka_persistence_journal_foo1' for 1 documents...
2019-06-07  INFO - Processing suffixed collection 'akka_persistence_journal_foo2' for 24 documents...
2019-06-07  INFO - Processing suffixed collection 'akka_persistence_journal_foo3' for 74013 documents...
2019-06-07  INFO - Processing suffixed collection 'akka_persistence_journal_foo4' for 54845 documents...
2019-06-07  INFO - 1 records were handled for suffixed collection 'akka_persistence_journal_foo1'
2019-06-07  INFO - 1/1 records were successfully transferred to 'akka_persistence_journal_foo1'
2019-06-07  INFO - 1/1 records, previously transferred to 'akka_persistence_journal_foo1', were successfully removed from 'akka_persistence_journal'
2019-06-07  INFO - 24 records were handled for suffixed collection 'akka_persistence_journal_foo2'
2019-06-07  INFO - 24/24 records were successfully transferred to 'akka_persistence_journal_foo2'
2019-06-07  INFO - 24/24 records, previously transferred to 'akka_persistence_journal_foo2', were successfully removed from 'akka_persistence_journal'
2019-06-07  INFO - 74013 records were handled for suffixed collection 'akka_persistence_journal_foo3'
2019-06-07  INFO - 74013/74013 records were successfully transferred to 'akka_persistence_journal_foo3'
2019-06-07  INFO - 74013/74013 records, previously transferred to 'akka_persistence_journal_foo3', were successfully removed from 'akka_persistence_journal'
2019-06-07  INFO - 54845 records were handled for suffixed collection 'akka_persistence_journal_foo4'
2019-06-07  INFO - 54845/54845 records were successfully transferred to 'akka_persistence_journal_foo4'
2019-06-07  INFO - 54845/54845 records, previously transferred to 'akka_persistence_journal_foo4', were successfully removed from 'akka_persistence_journal'
2019-06-07  INFO - JOURNALS: 128959 records were handled
2019-06-07  INFO - JOURNALS: 128883/128959 records were successfully transferred to suffixed collections
2019-06-07  INFO - JOURNALS: 128883/128959 records were successfully removed from 'akka_persistence_journal'collection
2019-06-07  INFO - JOURNALS: 76/128959 records were ignored and remain in 'akka_persistence_journal'
2019-06-07  INFO -

SNAPSHOTS: Gathering documents by suffixed collection names.  T h i s   m a y   t a k e   a   w h i l e  ! ! !   It may seem to freeze, be patient...

2019-06-07  INFO - Processing suffixed collection 'akka_persistence_snaps_foo3' for 101 documents...
2019-06-07  INFO - Processing suffixed collection 'akka_persistence_snaps_foo4' for 2 documents...
2019-06-07  INFO - 2 records were handled for suffixed collection 'akka_persistence_snaps_foo4'
2019-06-07  INFO - 2/2 records were successfully transferred to 'akka_persistence_snaps_foo4'
2019-06-07  INFO - 2/2 records, previously transferred to 'akka_persistence_snaps_foo4', were successfully removed from 'akka_persistence_snaps'
2019-06-07  INFO - 101 records were handled for suffixed collection 'akka_persistence_snaps_foo3'
2019-06-07  INFO - 101/101 records were successfully transferred to 'akka_persistence_snaps_foo3'
2019-06-07  INFO - 101/101 records, previously transferred to 'akka_persistence_snaps_foo3', were successfully removed from 'akka_persistence_snaps'
2019-06-07  INFO - SNAPSHOTS: 103 records were handled
2019-06-07  INFO - SNAPSHOTS: 103/103 records were successfully transferred to suffixed collections
2019-06-07  INFO - SNAPSHOTS: 103/103 records were successfully removed from 'akka_persistence_snaps'collection
2019-06-07  INFO - METADATA: all 106 records were successfully removed from 'akka_persistence_metadata' collection
2019-06-07  INFO - Automatic migration to collections with suffixed names has completed
```

* in case you choose the *heavy load* migration (in this example, *parallelism* = 2):
```
2019-06-07  INFO - Starting automatic migration to collections with suffixed names
This may take a while...
2019-06-07  INFO -

JOURNALS: Gathering documents by suffixed collection names.  T h i s   m a y   t a k e   a   w h i l e  ! ! !   It may seem to freeze, be patient...

2019-06-07  INFO - Processing persistence Id 'foo1' for 1 documents...
2019-06-07  INFO - Processing persistence Id 'foo2' for 24 documents...
2019-06-07  INFO - Persistence Id 'foo1' result: (inserted = 1, removed = 1, failed = 0)
2019-06-07  INFO - Persistence Id 'foo2' result: (inserted = 24, removed = 24, failed = 0)
2019-06-07  INFO - Processing persistence Id 'foo3' for 74013 documents...
2019-06-07  INFO - Processing persistence Id 'foo4' for 54845 documents...
2019-06-07  INFO - Persistence Id 'foo4' result: (inserted = 54845, removed = 54845, failed = 0)
2019-06-07  INFO - Persistence Id 'foo3' result: (inserted = 74013, removed = 74013, failed = 0)
2019-06-07  INFO - JOURNALS: 128959 records were handled
2019-06-07  INFO - JOURNALS: 128883/128959 records were successfully transferred to suffixed collections
2019-06-07  INFO - JOURNALS: 128883/128959 records were successfully removed from 'akka_persistence_journal'collection
2019-06-07  INFO - JOURNALS: 76/128959 records were ignored and remain in 'akka_persistence_journal'
2019-06-07  INFO -

SNAPSHOTS: Gathering documents by suffixed collection names.  T h i s   m a y   t a k e   a   w h i l e  ! ! !   It may seem to freeze, be patient...

2019-06-07  INFO - Processing persistence Id 'foo3' for 101 documents...
2019-06-07  INFO - Processing persistence Id 'foo4' for 2 documents...
2019-06-07  INFO - Persistence Id 'foo4' result: (inserted = 2, removed = 2, failed = 0)
2019-06-07  INFO - Persistence Id 'foo3' result: (inserted = 101, removed = 101, failed = 0)
2019-06-07  INFO - SNAPSHOTS: 103 records were handled
2019-06-07  INFO - SNAPSHOTS: 103/103 records were successfully transferred to suffixed collections
2019-06-07  INFO - SNAPSHOTS: 103/103 records were successfully removed from 'akka_persistence_snaps'collection
2019-06-07  INFO - METADATA: all 106 records were successfully removed from 'akka_persistence_metadata' collection
2019-06-07  INFO - Automatic migration to collections with suffixed names has completed
```

Notice that records **may** remain in unique collections "akka_persistence_journal" and "akka_persistence_snapshot" in case your `getSuffixfromPersistenceId` and `validateMongoCharacters` methods sometimes return an empty string. In that case, an information regarding these records is printed in the console above, and a warning is also printed if *inserted* records does not equal *removed* records.

Notice that unique collections "akka_persistence_journal" and "akka_persistence_snapshot" remain in the database, even if empty. You should remove them if you want, using mongo shell...

###### What's next ?
**Keep *suffixed collection names* feature enabled** as explained in [*suffixed collection names* usage](#suffixusage), and of course, **do not modify** your `getSuffixfromPersistenceId` and `validateMongoCharacters` methods.

Keep your database safe, **avoid running again the migration process**, so:
* remove migration code
* remove `akka-persistence-mongo-tools` dependency from your `build.sbt` file

That's it, you should **start your application** and enjoy *suffixed collection names* feature.
