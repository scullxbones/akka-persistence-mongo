# akka-persistence-mongo

[![Build Status](https://travis-ci.org/scullxbones/akka-persistence-mongo.png?branch=master)](https://travis-ci.org/scullxbones/akka-persistence-mongo)


## A MongoDB plugin for [akka-persistence](http://akka.io)

 * Three projects, a core and two driver implementations.  To use you must pull jars for both the common and one of the drivers:
   * common provides integration with Akka persistence, implementing the plugin API
   * casbah provides an implementation against the casbah driver (ONLY FUNCTIONAL APPROACH CURRENTLY)
   * rxmongo provides an implementation against the ReactiveMongo driver (NOT FUNCTIONAL ATM)
 * ~~No these projects are not available in Maven Central ... yet~~
 * Akka persistence has ~~an unstable~~ a much more stable api that is ~~changing with each release - do not expect this to work with non-matching versions of Akka until that changes~~ pretty stable as of Akka 2.3.0.
 * Both the journal and snapshot will reuse the dispatcher of the actor that is performing journalling and snapshot activities for any futures; this means you should *not* use the default dispatcher, but a unique dispatcher.  ~~This will be taken care of for you in the future.~~  This is now configured by default and is only an FYI.
 * The tests will automatically download mongodb via flapdoodle's embedded mongo utility, do not be alarmed :)

### Outstanding tasks:

 - ~~Solve Travis CI / embedded mongo issue~~
 - ~~Address dispatchers used~~
 - DRY up circuit breaker usage
 - ~~Publish to maven central~~
 - Finish implementation of RXMongo driver (currently blocked by the Akka version RxMongo uses, 2.2)

### What's new?

#### 0.0.5
 - A `what's new` section in the README :)
 - [Metrics Scala](https://github.com/erikvanoosten/metrics-scala) introduced on casbah journal ... [details](#metrics)

### Jars now available in central snapshots repo:

Version `0.0.5` is tracking Akka `2.3.0` and passing the [Akka Persistence TCK](https://github.com/krasserm/akka-persistence-testkit) version `0.2`

#### Using sbt?

```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-casbah" % "0.0.5"
```

#### Using Maven?

```xml
<dependency>
    <groupId>com.github.scullxbones</groupId>
    <artifactId>akka-persistence-mongo-casbah_2.10</artifactId>
    <version>0.0.5</version>
</dependency>
```

#### Using Gradle?
```groovy
compile 'com.github.scullxbones:akka-persistence-mongo-casbah_2.10:0.0.5'
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
	      snaps-collection = "akka_persistsence_snaps"
	      snaps-index = "akka_persistence_snaps_index"
	      snaps-write-concern = "Journaled"
	
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

### <a name="metrics"></a> Metrics

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
