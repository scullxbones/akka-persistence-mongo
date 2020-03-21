package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}

class RxMongoDriverSettingsSpec extends BaseUnitTest {
  import concurrent.duration._

  def reference: Config = ConfigFactory.load()

  def linear: Config = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.rxmongo.failover.growth = lin
    """.stripMargin)

  def exponential: Config = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.rxmongo.failover.growth = exp
      |akka.contrib.persistence.mongodb.rxmongo.failover.factor = 3
      |    """.stripMargin)

  def delayRetries: Config = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.rxmongo.failover.retries = 20
      |akka.contrib.persistence.mongodb.rxmongo.failover.initialDelay = 10ms
    """.stripMargin)

  def fixture[A](config: Config)(testCode: RxMongoDriverSettings => A): A = {
    testCode(RxMongoDriverSettings(new ActorSystem.Settings(getClass.getClassLoader, config.withFallback(ConfigFactory.defaultReference()), "settings name")))
  }

  "A settings object" should "correctly load the defaults" in fixture(reference) { s =>
    s.ExponentialGrowth shouldBe false
    s.LinearGrowth shouldBe false
    s.ConstantGrowth shouldBe true
    s.InitialDelay shouldBe 750.millis
    s.Factor shouldBe 1.5
    s.Retries shouldBe 10
  }

  it should "correctly load linear growth" in fixture(linear) { s =>
    s.LinearGrowth shouldBe true
  }

  it should "correctly load exponential growth" in fixture(exponential) { s =>
    s.ExponentialGrowth shouldBe true
    s.Factor shouldBe 3.0
  }

  it should "correctly load custom delay and retries" in fixture(delayRetries) { s =>
    s.InitialDelay shouldBe 10.millis
    s.Retries shouldBe 20
  }
}
