package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigException}

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

abstract class WithMongoPersistencePluginDispatcher(actorSystem: ActorSystem, config: Config) {

  implicit lazy val pluginDispatcher: ExecutionContextExecutor =
    Try(actorSystem.dispatchers.lookup(config.getString("plugin-dispatcher"))) match {
      case Success(configuredPluginDispatcher) =>
        configuredPluginDispatcher
      case Failure(_ : ConfigException) =>
        actorSystem.log.warning("plugin-dispatcher not configured for akka-contrib-mongodb-persistence. " +
          "Using actor system dispatcher.")
        actorSystem.dispatcher
    }
}
