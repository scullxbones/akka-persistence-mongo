package akka.contrib.persistence.mongodb

import com.typesafe.config._
import akka.actor.ActorSystem

class MongoSettingsSpec extends BaseUnitTest {

  def reference = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.mongo.driver = foo
    """.stripMargin)

  def withUri = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://appuser:apppass@localhost:27017/sample_db_name"
      |akka.contrib.persistence.mongodb.mongo.driver = foo
    """.stripMargin)

  def withMultiLegacy = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.mongo.urls = ["mongo1.example.com:27017","mongo2.example.com:27017"]
      |akka.contrib.persistence.mongodb.mongo.driver = foo
    """.stripMargin)

  def withMultiLegacyAndCreds = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.mongo.urls = ["mongo1.example.com:27017","mongo2.example.com:27017","mongo3.example.com:27017"]
      |akka.contrib.persistence.mongodb.mongo.username = my_user
      |akka.contrib.persistence.mongodb.mongo.password = my_pass
      |akka.contrib.persistence.mongodb.mongo.driver = foo
    """.stripMargin)

  def withCredentialsLegacy = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.mongo.urls = ["mongo1.example.com:27017"]
      |akka.contrib.persistence.mongodb.mongo.username = user
      |akka.contrib.persistence.mongodb.mongo.password = pass
      |akka.contrib.persistence.mongodb.mongo.db = spec_db
      |akka.contrib.persistence.mongodb.mongo.driver = foo
      """.stripMargin)

  def fixture[A](config: Config)(testCode: MongoSettings => A): A = {
    testCode(MongoSettings(new ActorSystem.Settings(getClass.getClassLoader,config,"settings name")))
  }

  "A settings object" should "correctly load the defaults" in fixture(reference) { s =>
    s.MongoUri shouldBe "mongodb://localhost:27017/akka-persistence"
  }

  it should "correctly load a uri" in fixture(withUri) { s =>
    s.MongoUri shouldBe "mongodb://appuser:apppass@localhost:27017/sample_db_name"
  }

  it should "correctly load a replica set" in fixture(withMultiLegacy) { s =>
    s.MongoUri shouldBe "mongodb://mongo1.example.com:27017,mongo2.example.com:27017/akka-persistence"
  }

  it should "correctly load a replica set with creds" in fixture(withMultiLegacyAndCreds) { s =>
    s.MongoUri shouldBe "mongodb://my_user:my_pass@mongo1.example.com:27017,mongo2.example.com:27017,mongo3.example.com:27017/akka-persistence"
  }

  it should "correctly load legacy credentials" in fixture(withCredentialsLegacy) { s =>
    s.MongoUri shouldBe "mongodb://user:pass@mongo1.example.com:27017/spec_db"
  }

  it should "allow for override" in fixture(withUri) { s =>
    val overridden = ConfigFactory.parseString("""
        |mongouri = "mongodb://localhost:27017/override"
      """.stripMargin)
    s.withOverride(overridden).MongoUri shouldBe "mongodb://localhost:27017/override"
    s.withOverride(overridden).Implementation shouldBe "foo"
    s.withOverride(overridden).JournalAutomaticUpgrade shouldBe false
  }
}
