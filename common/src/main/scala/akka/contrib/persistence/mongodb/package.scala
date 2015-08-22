package akka.contrib.persistence

package object mongodb {

  implicit class NonWrappingLongToInt(val pimped: Long) extends AnyVal {
    def toIntWithoutWrapping: Int = {
      if (pimped > Int.MaxValue) {
        Int.MaxValue
      } else {
        pimped.intValue
      }
    }
  }

}
