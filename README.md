# akka-persistence-mongo

[![Build Status](https://travis-ci.org/scullxbones/akka-persistence-mongo.png?branch=master)](https://travis-ci.org/scullxbones/akka-persistence-mongo)


## A MongoDB plugin for [akka-persistence](http://akka.io)

 * Three projects, a core and two driver implementations.  To use you must pull jars for *only one* of the drivers. Common will be pulled in as a transitive dependency:
   * common provides integration with Akka persistence, implementing the plugin API
   * casbah provides an implementation against the casbah driver
   * rxmongo provides an implementation against the ReactiveMongo driver
 * The tests will automatically download mongodb via flapdoodle's embedded mongo utility, do not be alarmed :)
 * Supports MongoDB major versions 2.4 and 2.6

### Outstanding tasks:

 - Tracked in issue log

### What's new?

#### 0.3.0
 - Migrated to build.sbt
 - [Direct passing](#direct) of bson documents into journal/snapshotter (#16), much thanks to @alari for the PR.

#### 0.2.4
 - Closes issue #18, thanks for the report and fixing pull request @marcuslinke!
   - If replay size exceeded `Int.MaxValue`, conversion was causing negative limits passed to rxmongo which are transformed to 101, much smaller than 4 billion.

#### 0.2.3
 - D'oh.  Skipped this one.

#### 0.2.2
 - Fixes #17, configuration no longer eagerly loads (this was providing no additional value)

#### 0.2.1
 - Snapshots no longer are forced to use Java serialization
   - `SelectedSnapshot` is not serialized directly
   - Payload is now wrapped in a `Snapshot` object
   - Fixes #15
 - Fix for auth test that fails irregularly

#### 0.2.0
 - Reactive Mongo driver now supported as an alternative to Casbah
   - Requires minimum of `0.10.5.0.akka23` of driver
   - Measurably (factor of 2ish) less performant than Casbah in limited load testing - not sure what to make of this yet, possibly different dispatcher tuning is necessary
   - Closes issue #7
 - It is important that you use only one driver sub-project at a time.  Using both in the same project is unsupported.
 - Compiled against Akka 2.3.8 and updated to latest of TCK
 - Now supports configuration of `wtimeout` and `fsync` for Journal and Snapshots write concerns

#### 0.1.4
 - Ignore sequence# hint during journaling max sequence query - in some cases it's quite wrong; Closes issue #14

#### 0.1.3
 - Verify against Mongo 2.6 - add support for both 2.6 and 2.4; Closes issue #13
 - DRY out usage of circuit breakers in preparation for RxMongo driver; Closes issue #6

#### 0.1.2
 - Close out connection pool with actor system shutdown; should fix leaking connections for use case of reusing single JVM with multiple `ActorSystem`s; Fixes issue #12

#### 0.1.1
 - Add support for authentication against Mongo (currently MONGO CR supported) - Issue #10
 - Compile against Akka 2.3.4, which is binary incompatible to 2.3.3 for akka-persistence module
 - Update to support 0.3.4 of [TCK](https://github.com/krasserm/akka-persistence-testkit) - fixing issue #8 indirectly

#### 0.1.0
 - Cross compile to 2.11 (Issue #9)
 - Update to 2.11 compatible versions of libraries (scalatest, casbah); Mark akka dependencies `provided`
 - Update to support 0.3.1 of [TCK](https://github.com/krasserm/akka-persistence-testkit), which supports Scala 2.11
 - Eliminate publish message from root project
 - Remove build and publish of rxmongo project for now

#### 0.0.9
 - Use batch mode for inserts to journal for very significant performance gain
 - Currently batch-mode inserts are not atomic, and so there is an opportunity for a failure to leave the journal in a partially completed state; Looking to address this with Issue #5

#### 0.0.8
 - Update to support 0.3 of [TCK](https://github.com/krasserm/akka-persistence-testkit), which added snapshot coverage

#### 0.0.7
 - Fix metrics bug by which all timers were shunted to the JE timer

#### 0.0.6
 - Fixed goofy name for extension - now is `MongoPersistenceExtension`, matching the [metrics docs](#metrics) below

#### 0.0.5
 - A `what's new` section in the README :)
 - [Metrics Scala](https://github.com/erikvanoosten/metrics-scala) introduced on casbah journal ... [details](#metrics)

### Jars now available in central snapshots repo:

Version `0.2.2` is tracking Akka `2.3.8` as a `provided` dependency and passing the TCK now delivered with Akka

Driver dependencies are also `provided`, meaning they must be included in the application project's dependencies.

#### Using sbt?

##### Casbah
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-casbah" % "0.2.2"
```

##### Reactive Mongo
```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-rxmongo" % "0.2.2"
```

#### Using Maven?

##### Casbah
```xml
<dependency>
    <groupId>com.github.scullxbones</groupId>
    <artifactId>akka-persistence-mongo-casbah_2.10</artifactId>
    <version>0.2.2</version>
</dependency>
```

##### Reactive Mongo
```xml
<dependency>
    <groupId>com.github.scullxbones</groupId>
    <artifactId>akka-persistence-mongo-rxmongo_2.10</artifactId>
    <version>0.2.2</version>
</dependency>
```


#### Using Gradle?

##### Casbah
```groovy
compile 'com.github.scullxbones:akka-persistence-mongo-casbah_2.10:0.2.2'
```

##### Reactive Mongo
```groovy
compile 'com.github.scullxbones:akka-persistence-mongo-rxmongo_2.10:0.2.2'
```

### How to use with akka-persistence?

Inside of your `application.conf` file, add the following line if you want to use the journal:

```
akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
```

Add the following line if you want to use the snapshotting functionality:

```
akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
```

### How to configure?

The defaults are fairly sane, but some common ones to change may be the location of the mongodb server.  These changes can be made in the `application.conf` file.

The only thing you have to provide is the driver implementation:

```
akka.contrib.persistence.mongodb.mongo.driver = "akka.contrib.persistence.mongodb.RxMongoPersistenceExtension" // or CasbahPersistenceExtension
```

#### Mongo URLs

A list of urls can be supplied.  These are specified as host:port.  For example `localhost:27017`

```
akka.contrib.persistence.mongodb.mongo.urls = ["192.168.0.1:27017","192.168.0.2:27017"]
```

#### Mongo DB, Collection, Index settings

A DB name can be specified, as can the names of the collections and indices used (one for journal, one for snapshots).

```
akka.contrib.persistence.mongodb.mongo.db = "my_akka_persistence"
akka.contrib.persistence.mongodb.mongo.journal-collection = "my_persistent_journal"
akka.contrib.persistence.mongodb.mongo.journal-index = "my_journal_index"
akka.contrib.persistence.mongodb.mongo.snaps-collection = "my_persistent_snapshots"
akka.contrib.persistence.mongodb.mongo.snaps-index = "my_snaps_index"
akka.contrib.persistence.mongodb.mongo.journal-write-concern = "Acknowledged"
```

#### Mongo authentication settings

Both username and password can be specified.  Currently MONGO-CR is the only authentication mode supported.

```
akka.contrib.persistence.mongodb.mongo.username = "my.mongo.user"
akka.contrib.persistence.mongodb.mongo.password = "secret"
```

#### Mongo Write Concern settings

This is well described in the [MongoDB Write Concern Documentation](http://docs.mongodb.org/manual/core/write-concern/)

The write concern can be set both for the journal plugin as well as the snapshot plugin.  Every level of write concern is supported.  The possible concerns are listed below in decreasing safety:

 * `ReplicaAcknowledged` - requires a replica to acknowledge the write, this confirms that at least two servers have seen the write
 * `Journaled` <DEFAULT> - requires that the change be journaled on the server that was written to.  Other replicas may not see this write on a network partition
 * `Acknowledged` - also known as "Safe", requires that the MongoDB server acknowledges the write.  This does not require that the change be persistent anywhere but memory.
 * `Unacknowledged` - does not require the MongoDB server to acknowledge the write.  It may raise an error in cases of network issues.  This was the default setting that MongoDB caught a lot of flak for, as it masked errors in exchange for straight-line speed.  This is no longer a default in the driver.
 * `ErrorsIgnored` - !WARNING! Extremely unsafe.  This level may not be able to detect if the MongoDB server is even running.  It will also not detect errors such as key collisions.  This makes data loss likely.  In general, don't use this.

It is a bad idea&trade; to use anything less safe than `Acknowledged` on the journal.  The snapshots can be played a bit more fast and loose, but the recommendation is not to go below `Acknowledged` for any serious work.

As a single data point (complete with grain of salt!) on a MBP i5 w/ SSD with [Kodemaniak's testbed](https://github.com/kodemaniak/akka-persistence-throughput-test) and mongodb running on the same physical machine, the performance difference between:
 * `Journaled` and `Acknowledged` write concerns is two orders of magnitude
 * `Acknowledged` and both `Unacknowledged` and `ErrorsIgnored` write concerns is one order of magnitude

`Journaled` is a significant trade off of straight line performance for safety, and should be benchmarked vs. `Acknowledged` for your specific use case and treated as an engineering cost-benefit decision.  The argument for the unsafe pair of `Unacknowledged` and `ErrorsIgnored` vs. `Acknowledged` seems to be much weaker, although the functionality is left for the user to apply supersonic lead projectiles to their phalanges as necessary :).

In addition to the mode of write concern, the `wtimeout` and `fsync` parameters may be configured seperately for the journal and snapshot.  FSync cannot be used with Journaling, so it will be disabled in that case.

Default values below:
```
akka.contrib.persistence.mongodb.mongo.journal-wtimeout = 3s
akka.contrib.persistence.mongodb.mongo.journal-fsync = false
akka.contrib.persistence.mongodb.mongo.snaps-wtimeout = 3s
akka.contrib.persistence.mongodb.mongo.snaps-fsync = false
```

#### Circuit breaker settings

By default the circuit breaker is set up with a `maxTries` of 5, and `callTimeout` and `resetTimeout` of 5s.  [Akka's circuit breaker documentation](http://doc.akka.io/docs/akka/snapshot/common/circuitbreaker.html) covers in detail what these settings are used for.  In the context of this plugin, you can set these in the following way:

```
akka.contrib.persistence.mongodb.mongo.breaker.maxTries = 3
akka.contrib.persistence.mongodb.mongo.breaker.timeout.call = 3s
akka.contrib.persistence.mongodb.mongo.breaker.timeout.reset = 10s
```

#### Configuring the dispatcher used

The name `akka-contrib-persistence-dispatcher` is mapped to a typically configured `ThreadPoolExecutor` based dispatcher.  This is needed to support the `Future`s used to interact with MongoDB via Casbah.  More details on these settings can be found in the [Akka Dispatcher documentation](http://doc.akka.io/docs/akka/snapshot/scala/dispatchers.html).  For example the (by core-scaled) pool sizes can be set:

```
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-min = 10
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-factor = 10
akka-contrib-persistence-dispatcher.thread-pool-executor.core-pool-size-max = 20
```

#### Defaults
```
akka {
  contrib {
    persistence {
      mongodb {
        mongo {
          urls = [ "localhost:27017" ]
          db = "akka-persistence"

          journal-collection = "akka_persistence_journal"
          journal-index = "akka_persistence_journal_index"
          # Write concerns are one of: ErrorsIgnored, Unacknowledged, Acknowledged, Journaled, ReplicaAcknowledged
          journal-write-concern = "Journaled"
          journal-wtimeout = 3s
          journal-fsync = false

          snaps-collection = "akka_persistsence_snaps"
          snaps-index = "akka_persistence_snaps_index"
          snaps-write-concern = "Journaled"
          snaps-wtimeout = 3s
          snaps-fsync = false

          breaker {
            maxTries = 5
            timeout {
              call = 5s
              reset = 5s
            }
          }
        }
      }
    }
  }
}

akka-contrib-persistence-dispatcher {
  # Dispatcher is the name of the event-based dispatcher
  type = Dispatcher
  # What kind of ExecutionService to use
  executor = "thread-pool-executor"
  # Configuration for the thread pool
  thread-pool-executor {
    # minimum number of threads to cap factor-based core number to
    core-pool-size-min = 2
    # No of core threads ... ceil(available processors * factor)
    core-pool-size-factor = 2.0
    # maximum number of threads to cap factor-based number to
    core-pool-size-max = 10
  }
  # Throughput defines the maximum number of messages to be
  # processed per actor before the thread jumps to the next actor.
  # Set to 1 for as fair as possible.
  throughput = 100
}
```

### <a name="direct"></a> Passing DB objects directly into journal collection

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

This funcationality is also exposed for snapshots.

### <a name="metrics"></a> Metrics (optional functionality)

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

#### What is measured?
Timers:
 - Single journal entry
 - Journal entry range
 - Journal append
 - Journal range delete
 - Journal multi delete
 - Journal confirms
 - Journal replays
 - Journal max sequence number for processor query

Meters:
 - Rate of usage of `permanent` flag in deletes

Histograms:
 - Batch sizes used for: range read, appends, deletes, confirms

#### Future plans?
 - Adding metrics to snapshotter
 - Adding health checks to both
