# Akka Persistence MongoDB driver (Journal + Read Journal, Snapshots)

[![Join the chat at https://gitter.im/scullxbones/akka-persistence-mongo](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/scullxbones/akka-persistence-mongo?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

* Test suite verifies against MongoDB 3.6, 4.0, 4.2

### Preparing to migrate to Apache Pekko? Not to worry, an incremental upgrade is supported.

* Provides forwards compatibility for Akka / Pekko features that persist internal class names, allowing for you to safely roll back your application after migration.

### Using Akka 2.6? Use 3.x Series.
[![Build Status](https://travis-ci.com/scullxbones/akka-persistence-mongo.svg?branch=master)](https://travis-ci.org/scullxbones/akka-persistence-mongo)
![Maven Central 2.12](https://maven-badges.herokuapp.com/maven-central/com.github.scullxbones/akka-persistence-mongo-common_2.12/badge.svg)
![Maven Central 2.13](https://maven-badges.herokuapp.com/maven-central/com.github.scullxbones/akka-persistence-mongo-common_2.13/badge.svg)

[Docs](docs/akka26.md)

* Cross-compiled for 2.12 / 2.13 - Java 8 targeted
* Active development
* Latest release - `3.0.8` ~ compatible with Akka 2.6

### Using Akka 2.5? Use 2.x Series.
[![Build Status](https://travis-ci.com/scullxbones/akka-persistence-mongo.svg?branch=akka25)](https://travis-ci.org/scullxbones/akka-persistence-mongo)
![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.scullxbones/akka-persistence-mongo-common_2.11/badge.svg)

[Docs](docs/akka25.md)

* Cross-compiled for 2.11 / 2.12 / 2.13 - Java 8 targeted
* Bugfixes only
* Latest release - `2.3.2` ~ compatible with Akka 2.5.12+ (see [#179](https://github.com/scullxbones/akka-persistence-mongo/issues/179) for details)

### Using Akka 2.4? Use 1.x Series.
[![Build Status](https://travis-ci.com/scullxbones/akka-persistence-mongo.svg?branch=akka24)](https://travis-ci.org/scullxbones/akka-persistence-mongo)

[Docs](docs/akka24.md)

* Backward incompatible to 0.x series - details in docs
* Compiled for 2.11 / 2.12 - Java 8 targeted
* No enhancements going forward, bugfixes will continue
* Latest release - `1.4.3` ~ compatible with Akka 2.4 (2.4.2+ for read journals)

### Using Akka 2.3? Use 0.x Series.
[![Build Status](https://travis-ci.com/scullxbones/akka-persistence-mongo.svg?branch=akka23)](https://travis-ci.org/scullxbones/akka-persistence-mongo)

[Docs](docs/akka23.md)

* Cross compiled 2.10/2.11 - Java 7 targeted
* No enhancements going forward, bugfixes will continue
* Latest release - `0.4.2`
