package akka.contrib.persistence.mongodb

sealed abstract class ServerVersion(val major: Double) {
  def minor: String
  def atLeast(min: ServerVersion): Boolean =
    ServerVersion.Ordering.gteq(this, min)
}
object ServerVersion {
  private val extract = "^(\\d\\.\\d)\\.(.*)".r
  def unapply(version: String): Option[ServerVersion] = {
    version match {
      case extract(major, minor) if major == "3.6" => Option(`3.6`(minor))
      case extract(major, minor) if major == "4.0" => Option(`4.0`(minor))
      case extract(major, minor) if major == "4.2" => Option(`4.2`(minor))
      case extract(major, minor) => Option(Unsupported(major.toDouble, minor))
      case _ => None
    }
  }

  final case class `3.6`(minor: String) extends ServerVersion(3.6)
  final val `3.6.0` = `3.6`("0")
  final case class `4.0`(minor: String) extends ServerVersion(4.0)
  final val `4.0.0` = `4.0`("0")
  final case class `4.2`(minor: String) extends ServerVersion(4.2)
  final val `4.2.0` = `4.2`("0")
  final case class Unsupported(_major: Double, minor: String) extends ServerVersion(_major)

  implicit object Ordering extends Ordering[ServerVersion] {
    override def compare(x: ServerVersion, y: ServerVersion): Int = {
      val major = x.major.compareTo(y.major)
      if (major == 0)
        compareMinors(x.minor, y.minor)
      else
        major
    }

    private def compareMinors(minorX: String, minorY: String): Int = {
      minorX.split('.').zipAll(minorY.split('.'), "0", "0").foldLeft(0){ case (last, (x,y)) =>
        if (last == 0) {
          x.toInt.compareTo(y.toInt)
        } else last
      }
    }
  }
}
