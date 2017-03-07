package akka.contrib.persistence

import java.util.concurrent.TimeUnit

import akka.actor.DynamicAccess
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

package object mongodb {

  type LoadClass = DynamicAccess

  implicit class NonWrappingLongToInt(val pimped: Long) extends AnyVal {
    def toIntWithoutWrapping: Int = {
      if (pimped > Int.MaxValue) {
        Int.MaxValue
      } else {
        pimped.intValue
      }
    }
  }

  implicit class ConfigWithFiniteDuration(val config: Config) extends AnyVal{
    def getFiniteDuration(path: String): FiniteDuration = {
      val d = config.getDuration(path)
      FiniteDuration(d.toMillis, TimeUnit.MILLISECONDS)
    }
  }

}
