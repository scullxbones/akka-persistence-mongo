package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Await
import scala.util.Try

trait BaseUnitTest extends FlatSpecLike with MockitoSugar with Matchers with PatienceConfiguration {

  override lazy val spanScaleFactor: Double = ConfigFactory.load().getDouble("akka.test.timefactor")

}

object ConfigLoanFixture {
  import concurrent.duration._

  def withConfig[T](config: Config, configurationRoot: String, name: String = "unit-test")(testCode: ((ActorSystem,Config)) => T):T = {
    implicit val actorSystem: ActorSystem = ActorSystem(name,config)
    val overrides = Try(config.getConfig(configurationRoot)).toOption.getOrElse(ConfigFactory.empty())
    try {
      testCode( (actorSystem, overrides) )
    } finally {
      actorSystem.terminate()
      Await.ready(actorSystem.whenTerminated, 3.seconds.dilated)
      ()
    }
  }
}