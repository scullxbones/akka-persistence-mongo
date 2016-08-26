package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.FlatSpecLike

import scala.concurrent.Await
import scala.util.Try

trait BaseUnitTest extends FlatSpecLike with MockitoSugar with Matchers

object ConfigLoanFixture {
  import concurrent.duration._

  def withConfig[T](config: Config, configurationRoot: String, name: String = "unit-test")(testCode: ((ActorSystem,Config)) => T):T = {
    val actorSystem: ActorSystem = ActorSystem(name,config)
    val overrides = Try(config.getConfig(configurationRoot)).toOption.getOrElse(ConfigFactory.empty())
    try {
      testCode( (actorSystem, overrides) )
    } finally {
      actorSystem.terminate()
      Await.ready(actorSystem.whenTerminated, 3.seconds)
      ()
    }
  }
}