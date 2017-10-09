## Changelog for 2.x major version

### 2.0.4
* Remove circuit breaker - currently redundant with breaker provided by akka-persistence layer [#168](https://github.com/scullxbones/akka-persistence-mongo/issues/168)
* Properly document rxmongo akka-stream requirement [#166](https://github.com/scullxbones/akka-persistence-mongo/issues/166)
* Don't incur cost of extra `createIndices` calls for single-collection (default) configuration [#169](https://github.com/scullxbones/akka-persistence-mongo/issues/169)

### 2.0.3
* Add tagging/query by tag support; make some strides on repeatably passing tests in CI by running project tests in serial
[#37](https://github.com/scullxbones/akka-persistence-mongo/issues/37)

### 2.0.2
* Retry fixing 151
[#151](https://github.com/scullxbones/akka-persistence-mongo/issues/151)

### 2.0.1
* Fix for timing-based error upon deleting journal entries [#151](https://github.com/scullxbones/akka-persistence-mongo/issues/151)

### 2.0.0
* Initial support of akka 2.5
