# akka-persistence-mongo
======

[![Build Status](https://travis-ci.org/scullxbones/akka-persistence-mongo.png?branch=master)](https://travis-ci.org/scullxbones/akka-persistence-mongo)


## An implementation of mongodb flavor of [akka-persistence](http://akka.io)

 * Three projects, a core and two driver implementations.  You must build both the core and one of the drivers:
   * common provides integration with Akka persistence, implementing the plugin API
   * casbah provides an implementation against the casbah driver (ONLY FUNCTIONAL APPROACH CURRENTLY)
   * rxmongo provides an implementation against the ReactiveMongo driver (NOT FUNCTIONAL ATM)
 * ~~No these projects are not available in Maven Central ... yet~~
 * Akka persistence has an unstable api that is changing with each release - do not expect this to work with non-matching versions of Akka until that changes
 * Both the journal and snapshot will reuse the dispatcher of the actor that is performing journalling and snapshot activities for any futures; this means you should *not* use the default dispatcher, but a unique dispatcher.  ~~This will be taken care of for you in the future.~~  This is now configured by default and is only an FYI.
 * The tests will automatically download mongodb via flapdoodle's embedded mongo utility, do not be alarmed :)

### Outstanding tasks:

 - ~~Solve Travis CI / embedded mongo issue~~
 - ~~Address dispatchers used~~
 - DRY up circuit breaker usage
 - ~~Publish to maven central~~
 - Finish implementation of RXMongo driver (currently blocked by the Akka version RxMongo uses, 2.2)

### Jars now available in central snapshots repo:

0.0.2 is tracking 2.3.0-RC4 and passing the [Akka Persistence TCK](https://github.com/krasserm/akka-persistence-testkit) version `0.1`

#### Using sbt?

```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-casbah" % "0.0.2"
```

#### Using Maven?

```xml
<dependency>
    <groupId>com.github.scullxbones</groupId>
    <artifactId>akka-persistence-mongo-casbah_2.10</artifactId>
    <version>0.0.2</version>
</dependency>
```

#### Using Gradle?
```groovy
runtime 'com.github.scullxbones:akka-persistence-mongo-casbah_2.10:0.0.2'
```

### How to use with akka-persistence?

Inside of your `application.conf` file, add the following line if you want to use the journal:

```
akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
```

Add the following line if you want to use the snapshotting functionality:

```
akka.persistence.snapshot.plugin = "akka-contrib-mongodb-persistence-snapshot"
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

#### Circuit breaker settings

By default the circuit breaker is set up with a `maxTries` of 5, and `callTimeout` and `resetTimeout` of 5s.  [Akka's circuit breaker documentation](http://doc.akka.io/docs/akka/snapshot/common/circuitbreaker.html) covers in detail what these settings are used for.  In the context of this plugin, you can set these in the following way:

```
akka.contrib.persistence.mongodb.mongo.breaker.maxTries = 3
akka.contrib.persistence.mongodb.mongo.breaker.timeout.call = 3s
akka.contrib.persistence.mongodb.mongo.breaker.timeout.reset = 10s
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
```
