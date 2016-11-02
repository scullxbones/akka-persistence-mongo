package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

class CasbahDriverSettingsSpec extends BaseUnitTest{

  def fixture[A](config: Config)(testCode: CasbahDriverSettings => A): A = {
    testCode(CasbahDriverSettings(new ActorSystem.Settings(getClass.getClassLoader, config, "settings name")))
  }

  def reference = ConfigFactory.load()

  "A settings object" should "correctly load the defaults" in fixture(reference){ s =>

    s.MinConnectionsPerHost shouldBe 0
    s.ConnectionsPerHost shouldBe 100
    s.ThreadsAllowedToBlockforConnectionMultiplier shouldBe 5
    s.ServerSelectionTimeout shouldBe 30000
    s.MaxWaitTime shouldBe 120000
    s.MaxConnectionLifeTime shouldBe 0
    s.MaxConnectionIdleTime shouldBe 0
    s.ConnectTimeout shouldBe 10000
    s.SocketTimeout shouldBe 0
    s.SocketKeepAlive shouldBe false
    s.SslEnabled shouldBe false
    s.SslInvalidHostNameAllowed shouldBe false
    s.HeartbeatFrequency shouldBe 10000
    s.MinHeartbeatFrequency shouldBe 500
    s.HeartbeatConnectTimeout shouldBe 20000
    s.HeartbeatSocketTimeout shouldBe 20000

  }

}
