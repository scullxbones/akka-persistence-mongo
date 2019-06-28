package akka.contrib.persistence.mongodb

import java.util.concurrent.TimeUnit

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.mongodb.{Block, ConnectionString}
import com.typesafe.config.Config
import org.mongodb.scala.MongoClientSettings
import org.mongodb.scala.connection._

object ScalaDriverSettings extends ExtensionId[ScalaDriverSettings] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): ScalaDriverSettings = {
      val fullPath = s"${getClass.getPackage.getName}.official"
      val systemConfig = system.settings.config
      new ScalaDriverSettings(systemConfig.getConfig(fullPath))
  }

  override def lookup(): ExtensionId[ScalaDriverSettings] = ScalaDriverSettings

  implicit def configbuilder2block[A](fn: A => A): Block[A] = new ConfigBuilderToBlock[A](fn)

  private class ConfigBuilderToBlock[A](fn: A => A) extends Block[A] {
    def apply(a: A): Unit = {
      fn(a)
      ()
    }
  }

  def builder[A](fn: A => A): A => A = identity[A]
}

class ScalaDriverSettings(config: Config) extends OfficialDriverSettings(config) with Extension {
  import ScalaDriverSettings._
  import scala.language.implicitConversions

  def configure(b: MongoClientSettings.Builder): MongoClientSettings.Builder = {
    /*
        TODO: Apparently unsupported in latest driver

        .socketKeepAlive(SocketKeepAlive)
        .heartbeatConnectTimeout(HeartbeatConnectTimeout.toMillis.toIntWithoutWrapping)
        .heartbeatSocketTimeout(HeartbeatSocketTimeout.toMillis.toIntWithoutWrapping)
     */

    val bldr = b.applyToClusterSettings(
      builder[ClusterSettings.Builder](
        _.serverSelectionTimeout(ServerSelectionTimeout.toMillis, TimeUnit.MILLISECONDS)
         .maxWaitQueueSize(ThreadsAllowedToBlockforConnectionMultiplier)
      )
    ).applyToConnectionPoolSettings(
      builder[ConnectionPoolSettings.Builder](
        _.maxWaitTime(MaxWaitTime.toMillis, TimeUnit.MILLISECONDS)
          .maxConnectionIdleTime(MaxConnectionIdleTime.toMillis, TimeUnit.MILLISECONDS)
          .maxConnectionLifeTime(MaxConnectionLifeTime.toMillis, TimeUnit.MILLISECONDS)
          .minSize(MinConnectionsPerHost)
          .maxSize(ConnectionsPerHost)
      )
    ).applyToServerSettings(
      builder[ServerSettings.Builder](
        _.heartbeatFrequency(HeartbeatFrequency.toMillis, TimeUnit.MILLISECONDS)
         .minHeartbeatFrequency(MinHeartbeatFrequency.toMillis, TimeUnit.MILLISECONDS)
      )
    ).applyToSocketSettings(
      builder[SocketSettings.Builder](
        _.connectTimeout(ConnectTimeout.toMillis.toIntWithoutWrapping, TimeUnit.MILLISECONDS)
          .readTimeout(SocketTimeout.toMillis.toIntWithoutWrapping, TimeUnit.MILLISECONDS)
      )
    ).applyToSslSettings(
      builder[SslSettings.Builder](
        _.enabled(SslEnabled)
         .invalidHostNameAllowed(SslInvalidHostNameAllowed)
      )
    )

    if (SslEnabled) {
      bldr.streamFactoryFactory(NettyStreamFactoryFactory())
    } else bldr
  }

  def configureWithConnectionString(b: MongoClientSettings.Builder, c: ConnectionString): MongoClientSettings.Builder = {

    val bldr = b.applyConnectionString(c)
    .applyToClusterSettings(
      builder[ClusterSettings.Builder](
        _.serverSelectionTimeout(Option(c.getServerSelectionTimeout.toLong).getOrElse(ServerSelectionTimeout.toMillis), TimeUnit.MILLISECONDS)
          .maxWaitQueueSize(Option(c.getThreadsAllowedToBlockForConnectionMultiplier.toInt).getOrElse(ThreadsAllowedToBlockforConnectionMultiplier))
      )
    ).applyToConnectionPoolSettings(
      builder[ConnectionPoolSettings.Builder](
        _.maxWaitTime(Option(c.getMaxWaitTime.toLong).getOrElse(MaxWaitTime.toMillis), TimeUnit.MILLISECONDS)
          .maxConnectionIdleTime(Option(c.getMaxConnectionIdleTime.toLong).getOrElse(MaxConnectionIdleTime.toMillis), TimeUnit.MILLISECONDS)
          .maxConnectionLifeTime(Option(c.getMaxConnectionLifeTime.toLong).getOrElse(MaxConnectionLifeTime.toMillis), TimeUnit.MILLISECONDS)
          .minSize(Option(c.getMinConnectionPoolSize.toInt).getOrElse(MinConnectionsPerHost))
          .maxSize(Option(c.getMaxConnectionPoolSize.toInt).getOrElse(ConnectionsPerHost))
      )
    ).applyToServerSettings(
      builder[ServerSettings.Builder](
        _.heartbeatFrequency(Option(c.getHeartbeatFrequency.toLong).getOrElse(HeartbeatFrequency.toMillis), TimeUnit.MILLISECONDS)
          .minHeartbeatFrequency(MinHeartbeatFrequency.toMillis, TimeUnit.MILLISECONDS) // no 'minHeartbeatFrequency' in ConnectionString
      )
    ).applyToSocketSettings(
      builder[SocketSettings.Builder](
        _.connectTimeout(Option(c.getConnectTimeout.toLong).getOrElse(ConnectTimeout.toMillis).toIntWithoutWrapping, TimeUnit.MILLISECONDS)
          .readTimeout(Option(c.getSocketTimeout.toLong).getOrElse(SocketTimeout.toMillis).toIntWithoutWrapping, TimeUnit.MILLISECONDS)
      )
    ).applyToSslSettings(
      builder[SslSettings.Builder](
        _.enabled(Option[Boolean](c.getSslEnabled).getOrElse(SslEnabled))
          .invalidHostNameAllowed(Option[Boolean](c.getSslInvalidHostnameAllowed).getOrElse(SslInvalidHostNameAllowed))
      )
    )

    if (SslEnabled) {
      bldr.streamFactoryFactory(NettyStreamFactoryFactory())
    } else bldr
  }
}
