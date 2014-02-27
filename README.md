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

0.0.1 release is tracking Akka 2.3.0-RC2

0.0.2 snapshot is tracking Akka 2.3.0-RC3

0.0.2 release will track 2.3.0-RC4 and will pass the [Akka Persistence TCK](https://github.com/krasserm/akka-persistence-testkit)

#### Using sbt?

```scala
libraryDependencies +="com.github.scullxbones" %% "akka-persistence-mongo-casbah" % "0.0.1"
```

#### Using Maven?

```xml
<dependency>
    <groupId>com.github.scullxbones</groupId>
    <artifactId>akka-persistence-mongo-casbah_2.10</artifactId>
    <version>0.0.1</version>
</dependency>
```

#### Using Gradle?
```groovy
runtime 'com.github.scullxbones:akka-persistence-mongo-casbah_2.10:0.0.1'
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
