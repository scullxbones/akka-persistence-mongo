package akka.contrib.persistence.mongodb

import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

class OfficialDriverSettings(config: Config){
    def MinConnectionsPerHost: Int = config.getInt("minpoolsize")
    def ConnectionsPerHost: Int = config.getInt("maxpoolsize")
    def ServerSelectionTimeout: FiniteDuration = config.getFiniteDuration("serverselectiontimeout")
    def MaxWaitTime: FiniteDuration = config.getFiniteDuration("waitqueuetimeout")
    def MaxConnectionIdleTime: FiniteDuration = config.getFiniteDuration("maxidletime")
    def MaxConnectionLifeTime: FiniteDuration = config.getFiniteDuration("maxlifetime")
    def ConnectTimeout: FiniteDuration = config.getFiniteDuration("connecttimeout")
    def SocketTimeout: FiniteDuration = config.getFiniteDuration("sockettimeout")
    def SocketKeepAlive: Boolean = config.getBoolean("socketkeepalive")
    def HeartbeatFrequency: FiniteDuration = config.getFiniteDuration("heartbeatfrequency")
    def MinHeartbeatFrequency: FiniteDuration = config.getFiniteDuration("minheartbeatfrequency")
    def HeartbeatConnectTimeout: FiniteDuration = config.getFiniteDuration("heartbeatconnecttimeout")
    def HeartbeatSocketTimeout: FiniteDuration = config.getFiniteDuration("heartbeatsockettimeout")
    def ThreadsAllowedToBlockforConnectionMultiplier: Int = config.getInt("waitqueuemultiple")
    def SslEnabled: Boolean = config.getBoolean("ssl")
    def SslInvalidHostNameAllowed: Boolean = config.getBoolean("sslinvalidhostnameallowed")
}
