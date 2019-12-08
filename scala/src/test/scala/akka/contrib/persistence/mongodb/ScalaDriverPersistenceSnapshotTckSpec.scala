package akka.contrib.persistence.mongodb

import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class ScalaDriverPersistenceSnapshotTckSpec extends SnapshotTckSpec(classOf[ScalaDriverPersistenceExtension], "officialScalaSnapshotTck")

@RunWith(classOf[JUnitRunner])
class ScalaDriverPersistenceSuffixSnapshotTckSpec extends SnapshotTckSpec(classOf[ScalaDriverPersistenceExtension], "officialScalaSuffixSnapshotTck", SuffixCollectionNamesTest.extendedConfig)
