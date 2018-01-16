/*
 * Copyright (c) 2013-2018 Brian Scully
 * Copyright (c) 2018      Gael Breard, Orange: Fix issue #179 about actorRef serialization
 */
package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, ActorSystem, DynamicAccess, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.serialization.Snapshot
import akka.persistence.{PersistentRepr, SelectedSnapshot, SnapshotMetadata}
import akka.serialization.{Serialization, SerializationExtension}
import reactivemongo.bson._
import DefaultBSONHandlers._

object RxMongoSerializersExtension extends ExtensionId[RxMongoSerializers] with ExtensionIdProvider {
  override def lookup = RxMongoSerializersExtension

  override def createExtension(system: ExtendedActorSystem) =
    new RxMongoSerializers(system.dynamicAccess, system)

  override def get(system: ActorSystem): RxMongoSerializers = super.get(system)
}

object RxMongoSerializers {

  implicit class PimpedBSONDocument(val doc: BSONDocument) extends AnyVal {
    def as[A](key: String)(implicit ev: Manifest[A], reader: BSONReader[_ <: BSONValue, A]): A =
      doc.getAs[A](key)
        .getOrElse(throw new IllegalArgumentException(s"Could not deserialize required key $key of type ${ev.runtimeClass.getName}"))
  }

}

class RxMongoSerializers(dynamicAccess: DynamicAccess, actorSystem: ActorSystem) extends Extension {
  import RxMongoSerializers._

  implicit val loadClass: LoadClass = dynamicAccess
  private implicit val system: ActorSystem = actorSystem
  implicit val serialization = SerializationExtension(actorSystem)

  implicit val dt: DocumentType[BSONDocument] = new DocumentType[BSONDocument] { }

  object Version {
    def unapply(d: BSONDocument): Option[(Int,BSONDocument)] = {
      d.getAs[Int](JournallingFieldNames.VERSION).orElse(Option(0)).map(_ -> d)
    }
  }

  implicit object RxMongoSnapshotSerialization extends BSONDocumentReader[SelectedSnapshot] with BSONDocumentWriter[SelectedSnapshot] with SnapshottingFieldNames {

    override def read(doc: BSONDocument): SelectedSnapshot = {
      val content = doc.getAs[Array[Byte]](V1.SERIALIZED)
      if (content.isDefined) {
        serialization.deserialize(content.get, classOf[SelectedSnapshot]).get
      } else {
        val pid = doc.as[String](PROCESSOR_ID)
        val sn = doc.as[Long](SEQUENCE_NUMBER)
        val timestamp = doc.as[Long](TIMESTAMP)

        val content = doc.get(V2.SERIALIZED) match {
          case Some(b: BSONDocument) =>
            b
          case Some(_) =>
            val snapshot = doc.as[Array[Byte]](V2.SERIALIZED)
            val deserialized = serialization.deserialize(snapshot, classOf[Snapshot]).get
            deserialized.data
          case None =>
            throw new IllegalStateException(s"Snapshot unreadable, missing serialized snapshot field ${V2.SERIALIZED}")
        }

        SelectedSnapshot(SnapshotMetadata(pid,sn,timestamp),content)
      }
    }

    override def write(snap: SelectedSnapshot): BSONDocument = {
      val content: BSONValue = snap.snapshot match {
        case b: BSONDocument =>
          b
        case _ =>
          SerializationHelper.withTransportInformation(serialization.system) {
            BSON.write(serialization.serialize(Snapshot(snap.snapshot)).get)
          }
      }
      BSONDocument(PROCESSOR_ID -> snap.metadata.persistenceId,
        SEQUENCE_NUMBER -> snap.metadata.sequenceNr,
        TIMESTAMP -> snap.metadata.timestamp,
        V2.SERIALIZED -> content)
    }

    @deprecated("Use v2 write instead", "0.3.0")
    def legacyWrite(snap: SelectedSnapshot): BSONDocument = {
      val content = serialization.serialize(snap).get
      BSONDocument(PROCESSOR_ID -> snap.metadata.persistenceId,
        SEQUENCE_NUMBER -> snap.metadata.sequenceNr,
        TIMESTAMP -> snap.metadata.timestamp,
        V1.SERIALIZED -> content)
    }
  }

  implicit object JournalDeserializer extends CanDeserializeJournal[BSONDocument] with JournallingFieldNames {

    override def deserializeDocument(document: BSONDocument): Event = document match {
      case Version(1,doc) => deserializeVersionOne(doc)
      case Version(0,doc) => deserializeDocumentLegacy(doc)
      case Version(x,_) => throw new IllegalStateException(s"Don't know how to deserialize version $x of document")
      case _ => throw new IllegalStateException("Failed to read or default version field")
    }

    private def deserializeVersionOne(d: BSONDocument)(implicit serialization: Serialization, system: ActorSystem): Event =
      Event(
        pid = d.as[String](PROCESSOR_ID),
        sn = d.as[Long](SEQUENCE_NUMBER),
        payload = deserializePayload(
          d.get(PayloadKey).get,
          d.as[String](TYPE),
          d.getAs[BSONArray](TAGS).toList.flatMap(_.values.collect{ case BSONString(s) => s }).toSet,
          d.getAs[String](HINT),
          d.getAs[Int](SER_ID),
          d.getAs[String](SER_MANIFEST)
        ),
        sender = d.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption),
        manifest = d.getAs[String](MANIFEST),
        writerUuid = d.getAs[String](WRITER_UUID)
      )

    private def deserializePayload(b: BSONValue, clue: String, tags: Set[String], clazzName: Option[String], serializerId: Option[Int], serializedManifest: Option[String])(implicit serialization: Serialization): Payload = (clue,b) match {
      case ("ser",BSONBinary(bfr, _)) =>
        Serialized(bfr.readArray(bfr.size), clazzName.getOrElse(classOf[AnyRef].getName), tags, serializerId, serializedManifest)
      case ("bson",d:BSONDocument) => Bson(d, tags)
      case ("bin",BSONBinary(bfr, _)) => Bin(bfr.readArray(bfr.size), tags)
      case ("repr",BSONBinary(bfr, _)) => Legacy(bfr.readArray(bfr.size), tags)
      case ("s",BSONString(s)) => StringPayload(s, tags)
      case ("d",BSONDouble(d)) => FloatingPointPayload(d, tags)
      case ("l",BSONLong(l)) => FixedPointPayload(l, tags)
      case ("b",BSONBoolean(bln)) => BooleanPayload(bln, tags)
      case (x,y) => throw new IllegalArgumentException(s"Unknown hint $x or type for payload content $y")
    }


    private def deserializeDocumentLegacy(document: BSONDocument)(implicit serialization: Serialization, system: ActorSystem): Event = {
      val persistenceId = document.as[String](PROCESSOR_ID)
      val sequenceNr = document.as[Long](SEQUENCE_NUMBER)
      val tags = document.getAs[BSONArray](TAGS).toList.flatMap(_.values.collect{ case BSONString(s) => s }).toSet
      document.get(SERIALIZED) match {
        case Some(b: BSONDocument) =>
          Event(pid = persistenceId,
                sn = sequenceNr,
                payload = Bson(b.as[BSONDocument](PayloadKey), tags),
                sender = b.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption),
                manifest = None)
        case Some(ser: BSONBinary) =>
          val repr = serialization.deserialize(ser.byteArray, classOf[PersistentRepr])
            .getOrElse(throw new IllegalStateException(s"Unable to deserialize PersistentRepr for id $persistenceId and sequence number $sequenceNr"))
          Event[BSONDocument](useLegacySerialization = false)(repr).copy(pid = persistenceId, sn = sequenceNr)
        case Some(x) =>
          throw new IllegalStateException(s"Unexpected value $x for $SERIALIZED field in document for id $persistenceId and sequence number $sequenceNr")
        case None =>
          throw new IllegalStateException(s"Cannot find required field $SERIALIZED in document for id $persistenceId and sequence number $sequenceNr")
      }
    }
  }

  implicit object JournalSerializer extends CanSerializeJournal[BSONDocument] with JournallingFieldNames {

    override def serializeAtom(atom: Atom): BSONDocument = {
      Option(atom.tags).filter(_.nonEmpty).foldLeft(
        BSONDocument(
          PROCESSOR_ID -> atom.pid,
          FROM -> atom.from,
          TO -> atom.to,
          EVENTS -> BSONArray(atom.events.map(serializeEvent)),
          VERSION -> 1
        )
      ){ case(d,tags) => d.merge(TAGS -> serializeTags(tags)) }
    }

    import Producer._
    private def serializeEvent(event: Event): BSONDocument = {
      val doc = serializePayload(event.payload)(
        BSONDocument(VERSION -> 1, PROCESSOR_ID -> event.pid, SEQUENCE_NUMBER -> event.sn))
      (for {
        d <- Option(doc)
        d <- Option(event.tags).filter(_.nonEmpty).map(tags => d.merge(TAGS -> serializeTags(tags))).orElse(Option(d))
        d <- event.manifest.map(m => d.merge(MANIFEST -> m)).orElse(Option(d))
        d <- event.writerUuid.map(u => d.merge(WRITER_UUID -> u)).orElse(Option(d))
        d <- event.sender
                  .filterNot(_ == actorSystem.deadLetters)
                  .flatMap(serialization.serialize(_).toOption)
                  .map(BSON.write(_))
                  .map(b => d.merge(SenderKey -> b)).orElse(Option(d))
      } yield d).getOrElse(doc)
    }

    private def serializeTags(tags: Set[String]): BSONArray =
      BSONArray(tags.map(BSONString))

    private def serializePayload(payload: Payload)(document: BSONDocument) = {
      val asDoc = payload match {
        case Bson(doc: BSONDocument, _) => BSONDocument(PayloadKey -> doc)
        case Bin(bytes, _) => BSONDocument(PayloadKey -> bytes)
        case Legacy(bytes, _) => BSONDocument(PayloadKey -> bytes)
        case s: Serialized[_] =>
          BSONDocument(PayloadKey -> BSON.write(s.bytes),
                       HINT -> s.className,
                       SER_ID -> s.serializerId,
                       SER_MANIFEST -> s.serializedManifest)
        case StringPayload(str, _) => BSONDocument(PayloadKey -> str)
        case FloatingPointPayload(dbl, _) => BSONDocument(PayloadKey -> dbl)
        case FixedPointPayload(lng, _) => BSONDocument(PayloadKey -> lng)
        case BooleanPayload(bl, _) => BSONDocument(PayloadKey -> bl)
        case x => throw new IllegalArgumentException(s"Unable to serialize payload of type $x")
      }

      document.merge(BSONDocument(TYPE -> payload.hint).merge(asDoc))
    }
  }

}
