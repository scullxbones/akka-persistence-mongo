package akka.contrib.persistence

import java.util.concurrent.TimeUnit

import akka.actor.DynamicAccess
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
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

  implicit class OffsetWithObjectIdToo(val offsets: Offset.type) extends AnyVal {
    def objectId(hexStr: String, time: Long) =
      ObjectIdOffset(hexStr, time)
  }

  implicit object OffsetOrdering extends Ordering[Offset] {
    override def compare(x: Offset, y: Offset): Int = {
      (x,y) match {
        case (NoOffset, NoOffset)                 => 0
        case (NoOffset, _)                        => -1
        case (_, NoOffset)                        => 1
        case (Sequence(a), Sequence(b))           => a compareTo b
        case (_:Sequence, _)                      => 0 // Can't compare
        case (_, _:Sequence)                      => 0 // Can't compare
        case (TimeBasedUUID(a), TimeBasedUUID(b)) => a compareTo b
        case (_:TimeBasedUUID, _)                 => 0 // Can't compare
        case (_, _:TimeBasedUUID)                 => 0 // Can't compare
        case (a:ObjectIdOffset, b:ObjectIdOffset) => a compareTo b
        case (_:ObjectIdOffset, _)                => 0 // Can't compare
        case (_, _:ObjectIdOffset)                => 0 // Can't compare
      }
    }
  }
}
