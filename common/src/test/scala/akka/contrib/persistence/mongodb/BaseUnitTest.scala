package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.FlatSpecLike

import scala.concurrent.Await
import scala.concurrent.duration.Duration

trait BaseUnitTest extends FlatSpecLike with MockitoSugar with Matchers

object ConfigLoanFixture {
  def withConfig[T](config: Config, name: String = "unit-test")(testCode: ActorSystem => T):T = {
    val actorSystem: ActorSystem = ActorSystem(name,config)
    try {
      testCode(actorSystem)
    } finally {
      Await.result(actorSystem.whenTerminated, Duration.Inf)
    }
  }
}