package akka.contrib.persistence.mongodb

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.mongodb.casbah.MongoClientOptions
import com.typesafe.config.Config

object CasbahDriverSettings extends ExtensionId[CasbahDriverSettings] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): CasbahDriverSettings = {
    val fullName = s"${getClass.getPackage.getName}.casbah"
    val systemConfig = system.settings.config
    new CasbahDriverSettings(systemConfig.getConfig(fullName))
  }

  override def lookup(): ExtensionId[CasbahDriverSettings] = CasbahDriverSettings
}

class CasbahDriverSettings(config: Config) extends OfficialDriverSettings(config) with Extension {

  def configure: MongoClientOptions.Builder => MongoClientOptions.Builder = {
     _.minConnectionsPerHost(MinConnectionsPerHost)
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
