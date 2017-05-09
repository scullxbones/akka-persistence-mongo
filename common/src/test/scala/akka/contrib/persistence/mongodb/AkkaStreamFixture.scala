package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.scalatest.concurrent.ScalaFutures

trait AkkaStreamFixture extends BeforeAndAfterEach {
  self: Suite =>

  private var _system: ActorSystem = _
  private var _materializer: ActorMaterializer = _

  private def config = ConfigFactory.parseString(
    """
      |akka.extensions = []
    """.stripMargin).withFallback(ConfigFactory.load())

  implicit def system: ActorSystem = Option(_system).getOrElse(throw new IllegalStateException("AtorSystem not started yet"))
  implicit def materializer: Materializer = Option(_materializer).getOrElse(throw new IllegalStateException("Materializer not started yet"))


  override protected def beforeEach(): Unit = {
    super.beforeEach()
    _system = ActorSystem(s"test-${System.currentTimeMillis()}", config)
    _materializer = ActorMaterializer()
  }

  override protected def afterEach(): Unit = {
    _materializer.shutdown()
    _system.terminate()
    super.afterEach()
  }
}