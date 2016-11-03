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
  def ServerSelectionTimeout = config.getFiniteDuration("serverselectiontimeout")
  def MaxWaitTime = config.getFiniteDuration("waitqueuetimeout")
  def MaxConnectionIdleTime = config.getFiniteDuration("maxidletime")
  def MaxConnectionLifeTime = config.getFiniteDuration("maxlifetime")
  def ConnectTimeout = config.getFiniteDuration("connecttimeout")
  def SocketTimeout = config.getFiniteDuration("sockettimeout")
  def SocketKeepAlive = config.getBoolean("socketkeepalive")
  def HeartbeatFrequency = config.getFiniteDuration("heartbeatfrequency")
  def MinHeartbeatFrequency = config.getFiniteDuration("minheartbeatfrequency")
  def HeartbeatConnectTimeout = config.getFiniteDuration("heartbeatconnecttimeout")
  def HeartbeatSocketTimeout = config.getFiniteDuration("heartbeatsockettimeout")
  def ThreadsAllowedToBlockforConnectionMultiplier = config.getInt("waitqueuemultiple")
  def SslEnabled = config.getBoolean("ssl")
  def SslInvalidHostNameAllowed = config.getBoolean("sslinvalidhostnameallowed")

  def mongoClientOptionsBuilder: MongoClientOptions.Builder = {
    new MongoClientOptions.Builder()
        .minConnectionsPerHost(MinConnectionsPerHost)
        .connectionsPerHost(ConnectionsPerHost)
        .threadsAllowedToBlockForConnectionMultiplier(ThreadsAllowedToBlockforConnectionMultiplier)
        .serverSelectionTimeout(ServerSelectionTimeout.toMillis.toIntWithoutWrapping)
        .maxWaitTime(MaxWaitTime.toMillis.toIntWithoutWrapping)
        .maxConnectionIdleTime(MaxConnectionIdleTime.toMillis.toIntWithoutWrapping)
        .maxConnectionLifeTime(MaxConnectionLifeTime.toMillis.toIntWithoutWrapping)
        .connectTimeout(ConnectTimeout.toMillis.toIntWithoutWrapping)
        .socketTimeout(SocketTimeout.toMillis.toIntWithoutWrapping)
        .socketKeepAlive(SocketKeepAlive)
        .sslEnabled(SslEnabled)
        .sslInvalidHostNameAllowed(SslInvalidHostNameAllowed)
        .heartbeatFrequency(HeartbeatFrequency.toMillis.toIntWithoutWrapping)
        .minHeartbeatFrequency(MinHeartbeatFrequency.toMillis.toIntWithoutWrapping)
        .heartbeatConnectTimeout(HeartbeatConnectTimeout.toMillis.toIntWithoutWrapping)
        .heartbeatSocketTimeout(HeartbeatSocketTimeout.toMillis.toIntWithoutWrapping)
  }
}
