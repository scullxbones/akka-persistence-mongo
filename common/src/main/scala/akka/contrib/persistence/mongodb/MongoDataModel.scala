package akka.contrib.persistence.mongodb

import akka.actor.ActorRef
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.Serialization

import scala.collection.immutable.{Seq => ISeq}
import scala.util.{Failure, Success}

sealed trait Payload {
  type Content

  def hint: String
  def content: Content
}

trait DocumentType[D]

case class Bson[D: DocumentType](document: D) extends Payload {
  type Content = D

  val hint = "bson"
  val content = document
}

case class Serialized[C](bytes: Array[Byte], clazz: Class[C])(implicit ser: Serialization) extends Payload {
  type Content = C

  val hint = "ser"
  val content = ser.deserialize(bytes, clazz) match {
    case Success(deser) => deser
    case Failure(x) => throw x
  }
}

object Serialized {
  def apply(any: AnyRef)(implicit ser: Serialization) = {
    val clazz = any.getClass
    new Serialized(ser.serializerFor(clazz).toBinary(any),clazz)
  }
}

case class Bin(bytes: Array[Byte]) extends Payload {
  type Content = Array[Byte]

  val hint = "bin"
  val content = bytes
}

case class StringPayload(string: String) extends Payload {
  type Content = String
  val hint = "s"
  val content = string
}

case class DoublePayload(double: Double) extends Payload {
  type Content = Double
  val hint = "d"
  val content = double
}

case class FloatPayload(float: Float) extends Payload {
  type Content = Float
  val hint = "f"
  val content = float
}

case class LongPayload(long: Long) extends Payload {
  type Content = Long
  val hint = "l"
  val content = long
}

case class IntPayload(int: Int) extends Payload {
  type Content = Int
  val hint = "i"
  val content = int
}

case class ShortPayload(short: Short) extends Payload {
  type Content = Short
  val hint = "sh"
  val content = short
}

case class BytePayload(byte: Byte) extends Payload {
  type Content = Byte
  val hint = "by"
  val content = byte
}

case class CharPayload(char: Char) extends Payload {
  type Content = Char
  val hint = "c"
  val content = char
}

case class BooleanPayload(boolean: Boolean) extends Payload {
  type Content = Boolean
  val hint = "b"
  val content = boolean
}

object Payload {
  import language.implicitConversions

  implicit def bson2payload[D](document: D)(implicit ev: Manifest[D], dt: DocumentType[D]): Bson[D] = Bson(document)
  implicit def str2payload(string: String): StringPayload = StringPayload(string)
  implicit def dbl2payload(double: Double): DoublePayload = DoublePayload(double)
  implicit def flt2payload(float: Float): FloatPayload = FloatPayload(float)
  implicit def lng2payload(long: Long): LongPayload = LongPayload(long)
  implicit def int2payload(int: Int): IntPayload = IntPayload(int)
  implicit def shrt2payload(short: Short): ShortPayload = ShortPayload(short)
  implicit def bt2payload(byte: Byte): BytePayload = BytePayload(byte)
  implicit def chr2payload(char: Char): CharPayload = CharPayload(char)
  implicit def bln2payload(bool: Boolean): BooleanPayload = BooleanPayload(bool)
  implicit def bytes2payload(buf: Array[Byte]): Bin = Bin(buf)

  def apply[D](any: Any)(implicit ser: Serialization, ev: Manifest[D], dt: DocumentType[D]): Payload = any match {
    case d:D => Bson(d)
    case bytes: Array[Byte] => Bin(bytes)
    case str: String => StringPayload(str)
    case d: Double => DoublePayload(d)
    case f: Float => FloatPayload(f)
    case l: Long => LongPayload(l)
    case i: Int => IntPayload(i)
    case s: Short => ShortPayload(s)
    case b: Byte => BytePayload(b)
    case c: Char => CharPayload(c)
    case b: Boolean => BooleanPayload(b)
    case x:AnyRef => Serialized(x)
    case x => throw new IllegalArgumentException(s"Type for $x of ${x.getClass} is currently unsupported")
  }

  def apply[D](hint: String, any: Any, clazzName: Option[String])(implicit evs: Serialization, ev: Manifest[D], dt: DocumentType[D]):Payload = (hint,any) match {
    case ("ser",ser:Array[Byte]) if clazzName.isDefined =>
      val clazz = Class.forName(clazzName.get)
      Serialized(ser,clazz)
    case ("bson",d:D) => Bson(d)
    case ("bin",b:Array[Byte]) => Bin(b)
    case ("s",s:String) => StringPayload(s)
    case ("d",d:Double) => DoublePayload(d)
    case ("f",f:Float) => FloatPayload(f)
    case ("l",l:Long) => LongPayload(l)
    case ("i",i:Int) => IntPayload(i)
    case ("sh",s:Short) => ShortPayload(s)
    case ("by",b:Byte) => BytePayload(b)
    case ("c",c:Char) => CharPayload(c)
    case ("b",b:Boolean) => BooleanPayload(b)
    case (x,y) => throw new IllegalArgumentException(s"Unknown hint $x or type for payload content $y")
  }
}


case class Event(pid: String, sn: Long, payload: Payload, sender: Option[ActorRef] = None, manifest: Option[String] = None, writerUuid: Option[String] = None) {
  def toRepr = PersistentRepr(
    persistenceId = pid,
    sequenceNr = sn,
    payload = payload.content,
    sender = sender.orNull,
    manifest = manifest.getOrElse(PersistentRepr.Undefined),
    writerUuid = writerUuid.getOrElse(PersistentRepr.Undefined)
  )
}

object Event {
  def apply[D](repr: PersistentRepr)(implicit ser: Serialization, ev: Manifest[D], dt: DocumentType[D]): Event = Event(
    pid = repr.persistenceId,
    sn = repr.sequenceNr,
    payload = Payload(repr.payload),
    sender = Option(repr.sender),
    manifest = Option(repr.manifest).filterNot(_ == PersistentRepr.Undefined),
    writerUuid = Option(repr.writerUuid).filterNot(_ == PersistentRepr.Undefined)
  )

  implicit object EventOrdering extends Ordering[Event] {
    override def compare(x: Event, y: Event): Int = Ordering[Long].compare(x.sn,y.sn)
  }
}

case class Atom(pid: String, from: Long, to: Long, events: ISeq[Event])

object Atom {
  def apply[D](aw: AtomicWrite)(implicit ser: Serialization, ev: Manifest[D], dt: DocumentType[D]): Atom = {
    val pid = aw.payload.head.persistenceId
    val (minSn,maxSn) = aw.payload.foldLeft((Long.MaxValue,0L)) { case ((min,max),repr) =>
      val newMin = if (repr.sequenceNr < min) repr.sequenceNr else min
      val newMax = if (repr.sequenceNr > max) repr.sequenceNr else max
      (newMin, newMax)
    }
    Atom(pid = pid, from = minSn, to = maxSn, events = aw.payload.map(Event.apply(_)(ser,ev,dt)))
  }
}

