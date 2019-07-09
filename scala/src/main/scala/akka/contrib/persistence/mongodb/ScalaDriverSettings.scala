package akka.contrib.persistence.mongodb

import java.net.URI
import java.util.concurrent.TimeUnit

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.mongodb.{Block, ConnectionString}
import com.typesafe.config.Config
import org.mongodb.scala.MongoClientSettings
import org.mongodb.scala.connection._

import scala.language.implicitConversions
import scala.util.Try

object ScalaDriverSettings extends ExtensionId[ScalaDriverSettings] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): ScalaDriverSettings = {
      val fullPath = s"${getClass.getPackage.getName}.official"
      val systemConfig = system.settings.config
      new ScalaDriverSettings(systemConfig.getConfig(fullPath))
  }

  override def lookup(): ExtensionId[ScalaDriverSettings] = ScalaDriverSettings

}

class ScalaDriverSettings(config: Config) extends OfficialDriverSettings(config) with Extension {

  def configure(uri: String): MongoClientSettings.Builder = {

    def getLongQueryProperty(key: String): Option[Long] = getQueryProperty(key, _.toLong)

    def getIntQueryProperty(key: String): Option[Int] = getQueryProperty(key, _.toInt)

    def getBooleanQueryProperty(key: String): Option[Boolean] = getQueryProperty(key, _.toBoolean)

    def getQueryProperty[T](key: String, f: String => T): Option[T] = {
      Try {
        new URI(uri).getQuery.split('&').collectFirst {
          case s if s.toLowerCase.startsWith(key.toLowerCase) && s.indexOf('=') > 0 => f(s.substring(s.indexOf('=') + 1))
        }
      }.recover {
        case _: Throwable => None
      }.getOrElse(None)
    }

    val bldr: MongoClientSettings.Builder = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(uri))
      .applyToClusterSettings(new Block[ClusterSettings.Builder]{
        override def apply(t: ClusterSettings.Builder): Unit = {
          t.serverSelectionTimeout(getLongQueryProperty("serverselectiontimeoutms").getOrElse(ServerSelectionTimeout.toMillis), TimeUnit.MILLISECONDS)
            .maxWaitQueueSize(getIntQueryProperty("waitqueuemultiple").getOrElse(ThreadsAllowedToBlockforConnectionMultiplier) * getIntQueryProperty("maxpoolsize").getOrElse(ConnectionsPerHost))
        }
      }
    ).applyToConnectionPoolSettings(new Block[ConnectionPoolSettings.Builder]{
        override def apply(t: ConnectionPoolSettings.Builder): Unit = {
          t.maxWaitTime(getLongQueryProperty("waitqueuetimeoutms").getOrElse(MaxWaitTime.toMillis), TimeUnit.MILLISECONDS)
            .maxConnectionIdleTime(getLongQueryProperty("maxidletimems").getOrElse(MaxConnectionIdleTime.toMillis), TimeUnit.MILLISECONDS)
            .maxConnectionLifeTime(getLongQueryProperty("maxlifetimems").getOrElse(MaxConnectionLifeTime.toMillis), TimeUnit.MILLISECONDS)
            .minSize(getIntQueryProperty("minpoolsize").getOrElse(MinConnectionsPerHost))
            .maxSize(getIntQueryProperty("maxpoolsize").getOrElse(ConnectionsPerHost))
        }
      }
    ).applyToServerSettings(new Block[ServerSettings.Builder]{
        override def apply(t: ServerSettings.Builder): Unit = {
          t.heartbeatFrequency(getLongQueryProperty("heartbeatfrequencyms").getOrElse(HeartbeatFrequency.toMillis), TimeUnit.MILLISECONDS)
            .minHeartbeatFrequency(MinHeartbeatFrequency.toMillis, TimeUnit.MILLISECONDS) // no 'minHeartbeatFrequency' in ConnectionString
        }
      }
    ).applyToSocketSettings(new Block[SocketSettings.Builder] {
      override def apply(t: SocketSettings.Builder): Unit = {
          t.connectTimeout(getLongQueryProperty("connecttimeoutms").getOrElse(ConnectTimeout.toMillis).toIntWithoutWrapping, TimeUnit.MILLISECONDS)
            .readTimeout(getLongQueryProperty("sockettimeoutms").getOrElse(SocketTimeout.toMillis).toIntWithoutWrapping, TimeUnit.MILLISECONDS)
        }
      }
    ).applyToSslSettings(new Block[SslSettings.Builder]{
      override def apply(t: SslSettings.Builder): Unit = {
          t.enabled(getBooleanQueryProperty("ssl").getOrElse(SslEnabled))
            .invalidHostNameAllowed(getBooleanQueryProperty("sslinvalidhostnameallowed").getOrElse(SslInvalidHostNameAllowed))
        }
      }
    )

    if (SslEnabled) {
      bldr.streamFactoryFactory(NettyStreamFactoryFactory())
    } else bldr

  }  

}
