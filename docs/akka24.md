## A MongoDB plugin for [akka-persistence](http://akka.io)

 * Three projects, a core and two driver implementations.  To use you must pull jars for *only one* of the drivers. Common will be pulled in as a transitive dependency:
   * common provides integration with Akka persistence, implementing the plugin API
   * casbah provides an implementation against the casbah driver
   * rxmongo provides an implementation against the ReactiveMongo driver
 * The tests will automatically download mongodb via flapdoodle's embedded mongo utility, do not be alarmed :)
 * Supports MongoDB major versions 2.6 and 3.0
 * Compiled against scala `2.11`.  When `2.12` is released, will cross compile.  Waiting on dependent libraries to catch up 

