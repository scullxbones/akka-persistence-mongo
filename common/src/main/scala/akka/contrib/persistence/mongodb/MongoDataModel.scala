/*
 * Copyright (c) 2013-2018 Brian Scully
 * Copyright (c) 2018      Gael Breard, Orange: Fix issue #179 about actorRef serialization
 */
package akka.contrib.persistence.mongodb

import akka.actor.ActorRef
import akka.persistence.journal.Tagged
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
  def tags: Set[String]
}

trait DocumentType[D]

case class ObjectIdOffset(hexStr: String, time: Long) extends Offset with Ordered[ObjectIdOffset] {
  override def compare(that: ObjectIdOffset): Int = {
    time compare that.time match {
      case cmp if cmp != 0 =>
        cmp
      case _ =>
        hexStr compare that.hexStr
    }
  }
}

case class Bson[D: DocumentType](content: D, tags: Set[String]) extends Payload {
  type Content = D
  val hint = "bson"
}

case class Serialized[C <: AnyRef](bytes: Array[Byte],
                                   className: String,
                                   tags: Set[String],
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
  def apply(any: AnyRef, tags: Set[String])(implicit ser: Serialization, loadClass: LoadClass): Serialized[_ <: AnyRef] = {
    val clazz = any.getClass
    ser.findSerializerFor(any) match {
      case s:SerializerWithStringManifest =>
        new Serialized(s.toBinary(any), clazz.getName, tags, Some(s.identifier), Option(s.manifest(any)).filter(_ => s.includeManifest))
      case s =>
        new Serialized(s.toBinary(any), clazz.getName, tags, Some(s.identifier), None)
    }
  }
}

case class Legacy(bytes: Array[Byte], tags: Set[String])(implicit ser: Serialization) extends Payload {
  type Content = PersistentRepr
  val hint = "repr"

  lazy val content: PersistentRepr = {
    ser.serializerFor(classOf[PersistentRepr]).fromBinary(bytes).asInstanceOf[PersistentRepr]
  }
}

object Legacy {
  def apply(repr: PersistentRepr, tags: Set[String])(implicit ser: Serialization): Legacy = {
    Legacy(ser.findSerializerFor(repr).toBinary(repr), tags)
  }
}

case class Bin(content: Array[Byte], tags: Set[String]) extends Payload {
  type Content = Array[Byte]
  val hint = "bin"
}

case class StringPayload(content: String, tags: Set[String]) extends Payload {
  type Content = String
  val hint = "s"
}

object FloatingPointPayload {
  def apply[N:Numeric](value: N, tags: Set[String]): FloatingPointPayload =
    FloatingPointPayload(implicitly[Numeric[N]].toDouble(value), tags)
}

case class FloatingPointPayload(content: Double, tags: Set[String]) extends Payload {
  type Content = Double
  val hint = "d"
}

object FixedPointPayload {
  def apply[N:Numeric](value: N, tags: Set[String]): FixedPointPayload =
    FixedPointPayload(implicitly[Numeric[N]].toLong(value), tags)
}

case class FixedPointPayload(content: Long, tags: Set[String]) extends Payload {
  type Content = Long
  val hint = "l"
}

case class BooleanPayload(content: Boolean, tags: Set[String]) extends Payload {
  type Content = Boolean
  val hint = "b"
}

object Payload {

  import language.implicitConversions

  implicit def bson2payload[D](document: D)(implicit ev: Manifest[D], dt: DocumentType[D]): Bson[D] = Bson(document, Set.empty[String])

  implicit def str2payload(string: String): StringPayload = StringPayload(string, Set.empty[String])

  implicit def fpnum2payload(double: Double): FloatingPointPayload = FloatingPointPayload(double, Set.empty[String])

  implicit def fxnum2payload(long: Long): FixedPointPayload = FixedPointPayload(long, Set.empty[String])

  implicit def bln2payload(bool: Boolean): BooleanPayload = BooleanPayload(bool, Set.empty[String])

  implicit def bytes2payload(buf: Array[Byte]): Bin = Bin(buf, Set.empty[String])

  def apply[D](payload: Any, tags: Set[String] = Set.empty)(implicit ser: Serialization, ev: Manifest[D], dt: DocumentType[D], loadClass: LoadClass): Payload = {
    SerializationHelper.withTransportInformation[Payload](ser.system) {
      payload match {
        case tg: Tagged => Payload(tg.payload, tg.tags)
        case pr: PersistentRepr => Legacy(pr, tags)
        case d: D => Bson(d, tags)
        case bytes: Array[Byte] => Bin(bytes, tags)
        case str: String => StringPayload(str, tags)
        case n: Double => FloatingPointPayload(n, tags)
        case n: Long => FixedPointPayload(n, tags)
        case b: Boolean => BooleanPayload(b, tags)
        case x: AnyRef => Serialized(x, tags)
        case x => throw new IllegalArgumentException(s"Type for $x of ${x.getClass} is currently unsupported")
      }
    }
  }

  def apply[D](hint: String, any: Any, tags: Set[String], clazzName: Option[String],
               serId: Option[Int], serManifest: Option[String])
              (implicit evs: Serialization, ev: Manifest[D], dt: DocumentType[D], loadClass: LoadClass):Payload =
    (hint,any) match {
      case ("repr",repr:Array[Byte]) => Legacy(repr, tags)
      case ("ser",ser:Array[Byte]) =>
        Serialized(ser, clazzName.getOrElse(classOf[AnyRef].getName), tags, serId, serManifest)
      case ("bson",d:D) => Bson(d, tags)
      case ("bin",b:Array[Byte]) => Bin(b, tags)
      case ("s",s:String) => StringPayload(s, tags)
      case ("d",d:Double) => FloatingPointPayload(d, tags)
      case ("l",l:Long) => FixedPointPayload(l, tags)
      case ("b",b:Boolean) => BooleanPayload(b, tags)
      case (x,y) => throw new IllegalArgumentException(s"Unknown hint $x or type for payload content $y")
    }
}


case class Event(pid: String, sn: Long, payload: Payload, sender: Option[ActorRef] = None, manifest: Option[String] = None, writerUuid: Option[String] = None) {

  def tags: Set[String] = payload.tags

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

case class Atom(pid: String, from: Long, to: Long, events: ISeq[Event]) {
  def tags: Set[String] = events.foldLeft(Set.empty[String])(_ ++ _.tags)
}

object Atom {
  def apply[D](aw: AtomicWrite, useLegacySerialization: Boolean)(implicit ser: Serialization, ev: Manifest[D], dt: DocumentType[D], loadClass: LoadClass): Atom = {
    Atom(pid = aw.persistenceId,
      from = aw.lowestSequenceNr,
      to = aw.highestSequenceNr,
      events = aw.payload.map(Event.apply(useLegacySerialization)(_)))
  }
}

