package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.duration._

class CasbahDriverSettingsSpec extends BaseUnitTest{

  def fixture[A](config: Config)(testCode: CasbahDriverSettings => A): A = {
    testCode(CasbahDriverSettings(ActorSystem("casbah-driver-settings-spec", config, getClass.getClassLoader)))
  }

  def reference = ConfigFactory.load()

  def overriden = ConfigFactory.parseString(
    """
      |akka.contrib.persistence.mongodb.casbah.socketkeepalive = true
      |akka.contrib.persistence.mongodb.casbah.maxpoolsize = 5
      |akka.contrib.persistence.mongodb.casbah.heartbeatfrequency = 500ms
    """.stripMargin)

  "A settings object" should "correctly load the defaults" in fixture(reference){ s =>

    s.MinConnectionsPerHost shouldBe 0
    s.ConnectionsPerHost shouldBe 100
    s.ThreadsAllowedToBlockforConnectionMultiplier shouldBe 5
    s.ServerSelectionTimeout shouldBe 30.seconds
    s.MaxWaitTime shouldBe 2.minutes
    s.MaxConnectionLifeTime shouldBe 0.seconds
    s.MaxConnectionIdleTime shouldBe 0.seconds
    s.ConnectTimeout shouldBe 10.seconds
    s.SocketTimeout shouldBe 0.millis
    s.SocketKeepAlive shouldBe false
    s.SslEnabled shouldBe false
    s.SslInvalidHostNameAllowed shouldBe false
    s.HeartbeatFrequency shouldBe 10.seconds
    s.MinHeartbeatFrequency shouldBe 500.millis
    s.HeartbeatConnectTimeout shouldBe 20.seconds
    s.HeartbeatSocketTimeout shouldBe 20.seconds

  }

  it should "correctly load overriden values" in fixture(overriden){s =>
    s.SocketKeepAlive shouldBe true
    s.ConnectionsPerHost shouldBe 5
    s.HeartbeatFrequency shouldBe 500.millis
  }

}
