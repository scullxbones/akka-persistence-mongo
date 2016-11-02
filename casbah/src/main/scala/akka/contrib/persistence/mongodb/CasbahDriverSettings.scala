package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.casbah.MongoClientOptions
import com.typesafe.config.Config

object CasbahDriverSettings {

  def apply(systemSettings: ActorSystem.Settings) = {
    val fullName = s"${getClass.getPackage.getName}.casbah"
    val systemConfig = systemSettings.config
    new CasbahDriverSettings(systemConfig.getConfig(fullName))
  }

}

class CasbahDriverSettings(val config: Config){

  def MinConnectionsPerHost = config.getInt("minpoolsize")
  def ConnectionsPerHost = config.getInt("maxpoolsize")
  def ServerSelectionTimeout = config.getInt("serverselectiontimeoutms")
  def MaxWaitTime = config.getInt("waitqueuetimeoutms")
  def MaxConnectionIdleTime = config.getInt("maxidletimems")
  def MaxConnectionLifeTime = config.getInt("maxlifetimems")
  def ConnectTimeout = config.getInt("connecttimeoutms")
  def SocketTimeout = config.getInt("sockettimeoutms")
  def SocketKeepAlive = config.getBoolean("socketkeepalive")
  def HeartbeatFrequency = config.getInt("heartbeatfrequencyms")
  def MinHeartbeatFrequency = config.getInt("minheartbeatfrequencyms")
  def HeartbeatConnectTimeout = config.getInt("heartbeatconnecttimeoutms")
  def HeartbeatSocketTimeout = config.getInt("heartbeatsockettimeoutms")
  def ThreadsAllowedToBlockforConnectionMultiplier = config.getInt("waitqueuemultiple")
  def SslEnabled = config.getBoolean("ssl")
  def SslInvalidHostNameAllowed = config.getBoolean("sslinvalidhostnameallowed")

  def mongoClientOptionsBuilder: MongoClientOptions.Builder = {
    new MongoClientOptions.Builder()
        .minConnectionsPerHost(MinConnectionsPerHost)
        .connectionsPerHost(ConnectionsPerHost)
        .threadsAllowedToBlockForConnectionMultiplier(ThreadsAllowedToBlockforConnectionMultiplier)
        .serverSelectionTimeout(ServerSelectionTimeout)
        .maxWaitTime(MaxWaitTime)
        .maxConnectionIdleTime(MaxConnectionIdleTime)
        .maxConnectionLifeTime(MaxConnectionLifeTime)
        .connectTimeout(ConnectTimeout)
        .socketTimeout(SocketTimeout)
        .socketKeepAlive(SocketKeepAlive)
        .sslEnabled(SslEnabled)
        .sslInvalidHostNameAllowed(SslInvalidHostNameAllowed)
        .heartbeatFrequency(HeartbeatFrequency)
        .minHeartbeatFrequency(MinHeartbeatFrequency)
        .heartbeatConnectTimeout(HeartbeatConnectTimeout)
        .heartbeatSocketTimeout(HeartbeatSocketTimeout)
  }

}
