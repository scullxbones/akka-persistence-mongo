package akka.contrib.persistence.mongodb

import akka.actor.ActorRef
import akka.persistence.query.{EventEnvelope, Offset}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializerWithStringManifest}

import scala.collection.immutable.{Seq => ISeq}
import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

sealed trait Payload {
  type Content

  def hint: String
  def content: Content
}

trait DocumentType[D]

case class Bson[D: DocumentType](content: D) extends Payload {
  type Content = D
  val hint = "bson"
}

case class Serialized[C <: AnyRef](bytes: Array[Byte],
                                   className: String,
                                   serializerId: Option[Int],
                                   serializedManifest: Option[String])(implicit ser: Serialization, loadClass: LoadClass, ct: ClassTag[C]) extends Payload {
  type Content = C

  val hint = "ser"
  lazy val content: C = {

    val clazz = loadClass.getClassFor[X forSome { type X <: AnyRef }](className)

    val tried = (serializedManifest,serializerId,clazz.flatMap(c => Try(ser.serializerFor(c)))) match {
      // Manifest was serialized, class exists ~ prefer read-time configuration
      case (Some(manifest), _, Success(clazzSer)) =>
        ser.deserialize(bytes, clazzSer.identifier, manifest)

      // No manifest id serialized, prefer read-time configuration
      case (None, _, Success(clazzSer)) =>
        ser.deserialize[X forSome { type X <: AnyRef }](bytes, clazzSer.identifier, clazz.toOption)

      // Manifest, id were serialized, class doesn't exist - use write-time configuration
      case (Some(manifest), Some(id), Failure(_)) =>
        ser.deserialize(bytes, id, manifest)

      // Below cases very unlikely to succeed

      // No manifest id serialized, class doesn't exist - use write-time configuration
      case (None, Some(id),Failure(_)) =>
        ser.deserialize[X forSome { type X <: AnyRef }](bytes, id, clazz.toOption)

      // fall back
      case (_,None, Failure(_)) =>
        ser.deserialize(bytes, clazz.get)
    }

    tried match {
      case Success(deser) => deser.asInstanceOf[C]
      case Failure(x) => throw x
    }
  }
}

object Serialized {
  def apply(any: AnyRef)(implicit ser: Serialization, loadClass: LoadClass): Serialized[_ <: AnyRef] = {
    val clazz = any.getClass
    ser.findSerializerFor(any) match {
      case s:SerializerWithStringManifest =>
        new Serialized(s.toBinary(any), clazz.getName, Some(s.identifier), Option(s.manifest(any)).filter(_ => s.includeManifest))
      case s =>
        new Serialized(s.toBinary(any), clazz.getName, Some(s.identifier), None)
    }
  }
}

case class Legacy(bytes: Array[Byte])(implicit ser: Serialization) extends Payload {
  type Content = PersistentRepr
  val hint = "repr"

  lazy val content: PersistentRepr = {
    ser.serializerFor(classOf[PersistentRepr]).fromBinary(bytes).asInstanceOf[PersistentRepr]
  }
}

object Legacy {
  def apply(repr: PersistentRepr)(implicit ser: Serialization): Legacy = {
    Legacy(ser.findSerializerFor(repr).toBinary(repr))
  }
}

case class Bin(content: Array[Byte]) extends Payload {
  type Content = Array[Byte]
  val hint = "bin"
}

case class StringPayload(content: String) extends Payload {
  type Content = String
  val hint = "s"
}

object FloatingPointPayload {
  def apply[N:Numeric](value: N): FloatingPointPayload =
    FloatingPointPayload(implicitly[Numeric[N]].toDouble(value))
}

case class FloatingPointPayload(content: Double) extends Payload {
  type Content = Double
  val hint = "d"
}

object FixedPointPayload {
  def apply[N:Numeric](value: N): FixedPointPayload =
    FixedPointPayload(implicitly[Numeric[N]].toLong(value))
}

case class FixedPointPayload(content: Long) extends Payload {
  type Content = Long
  val hint = "l"
}

case class BooleanPayload(content: Boolean) extends Payload {
  type Content = Boolean
  val hint = "b"
}

object Payload {
  import language.implicitConversions

  implicit def bson2payload[D](document: D)(implicit ev: Manifest[D], dt: DocumentType[D]): Bson[D] = Bson(document)
  implicit def str2payload(string: String): StringPayload = StringPayload(string)
  implicit def fpnum2payload(double: Double): FloatingPointPayload = FloatingPointPayload(double)
  implicit def fxnum2payload(long: Long): FixedPointPayload = FixedPointPayload(long)
  implicit def bln2payload(bool: Boolean): BooleanPayload = BooleanPayload(bool)
  implicit def bytes2payload(buf: Array[Byte]): Bin = Bin(buf)

  def apply[D](payload: Any)(implicit ser: Serialization, ev: Manifest[D], dt: DocumentType[D], loadClass: LoadClass): Payload = payload match {
    case pr: PersistentRepr => Legacy(pr)
    case d:D                => Bson(d)
    case bytes: Array[Byte] => Bin(bytes)
    case str: String        => StringPayload(str)
    case n: Double          => FloatingPointPayload(n)
    case n: Long            => FixedPointPayload(n)
    case b: Boolean         => BooleanPayload(b)
    case x:AnyRef           => Serialized(x)
    case x                  => throw new IllegalArgumentException(s"Type for $x of ${x.getClass} is currently unsupported")
  }

  def apply[D](hint: String, any: Any, clazzName: Option[String],
               serId: Option[Int], serManifest: Option[String])
              (implicit evs: Serialization, ev: Manifest[D], dt: DocumentType[D], loadClass: LoadClass):Payload =
    (hint,any) match {
      case ("repr",repr:Array[Byte]) => Legacy(repr)
      case ("ser",ser:Array[Byte]) =>
        Serialized(ser, clazzName.getOrElse(classOf[AnyRef].getName), serId, serManifest)
      case ("bson",d:D) => Bson(d)
      case ("bin",b:Array[Byte]) => Bin(b)
      case ("s",s:String) => StringPayload(s)
      case ("d",d:Double) => FloatingPointPayload(d)
      case ("l",l:Long) => FixedPointPayload(l)
      case ("b",b:Boolean) => BooleanPayload(b)
      case (x,y) => throw new IllegalArgumentException(s"Unknown hint $x or type for payload content $y")
    }
}


case class Event(pid: String, sn: Long, payload: Payload, sender: Option[ActorRef] = None, manifest: Option[String] = None, writerUuid: Option[String] = None) {
  def toRepr: PersistentRepr = payload match {
    case l:Legacy =>
      l.content.update(persistenceId = pid, sequenceNr = sn)
    case x =>
      PersistentRepr(
        persistenceId = pid,
        sequenceNr = sn,
        payload = x.content,
        sender = sender.orNull,
        manifest = manifest.getOrElse(PersistentRepr.Undefined),
        writerUuid = writerUuid.getOrElse(PersistentRepr.Undefined)
      )
  }

  def toEnvelope(offset: Offset): EventEnvelope = payload match {
    case l:Legacy =>
      EventEnvelope(
        offset = offset,
        persistenceId = pid,
        sequenceNr = sn,
        event = l.content.payload
      )
    case x =>
      EventEnvelope(
        offset = offset,
        persistenceId = pid,
        sequenceNr = sn,
        event = x.content
      )
  }
}

object Event {
  def apply[D](useLegacySerialization: Boolean)(repr: PersistentRepr)(implicit ser: Serialization, ev: Manifest[D], dt: DocumentType[D], loadClass: LoadClass): Event =
  if (useLegacySerialization)
    Event(
      pid = repr.persistenceId,
      sn = repr.sequenceNr,
      payload = Payload(repr),
      sender = Option(repr.sender),
      manifest = Option(repr.manifest).filterNot(_ == PersistentRepr.Undefined),
      writerUuid = Option(repr.writerUuid).filterNot(_ == PersistentRepr.Undefined)
    )
  else
    Event(
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
  def apply[D](aw: AtomicWrite, useLegacySerialization: Boolean)(implicit ser: Serialization, ev: Manifest[D], dt: DocumentType[D], loadClass: LoadClass): Atom = {
    Atom(pid = aw.persistenceId,
      from = aw.lowestSequenceNr,
      to = aw.highestSequenceNr,
      events = aw.payload.map(Event.apply(useLegacySerialization)(_)))
  }
}

