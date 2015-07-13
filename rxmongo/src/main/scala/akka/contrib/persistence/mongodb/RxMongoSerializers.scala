package akka.contrib.persistence.mongodb

import akka.actor.{ActorSystem, ActorRef}
import akka.contrib.persistence.mongodb.JournallingFieldNames._
import akka.persistence.serialization.Snapshot
import akka.persistence.{SnapshotMetadata, SelectedSnapshot, PersistentRepr}
import akka.serialization.Serialization
import reactivemongo.bson._
import DefaultBSONHandlers._
import reactivemongo.bson.buffer.ArrayReadableBuffer

object RxMongoSerializers {

  implicit val dt: DocumentType[BSONDocument] = new DocumentType[BSONDocument] { }

  implicit class PimpedBSONDocument(val doc: BSONDocument) extends AnyVal {
    def as[A](key: String)(implicit ev: Manifest[A], reader: BSONReader[_ <: BSONValue, A]) =
      doc.getAs[A](key)
        .getOrElse(throw new IllegalArgumentException(s"Could not deserialize required key $key of type ${ev.runtimeClass.getName}"))
  }

  implicit object BsonBinaryWriter extends BSONWriter[Array[Byte], BSONBinary] {
    def write(t: Array[Byte]): reactivemongo.bson.BSONBinary =
      BSONBinary(ArrayReadableBuffer(t), Subtype.GenericBinarySubtype)
  }

  object Version {
    def unapply(d: BSONDocument): Option[(Int,BSONDocument)] = {
      d.getAs[Int](VERSION).orElse(Option(0)).map(_ -> d)
    }
  }

  class RxMongoSnapshotSerialization(implicit serialization: Serialization) extends BSONDocumentReader[SelectedSnapshot] with BSONDocumentWriter[SelectedSnapshot] {

    import SnapshottingFieldNames._

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
          case Some(v) =>
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
          BSON.write(serialization.serialize(Snapshot(snap.snapshot)).get)
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

  implicit object JournalDeserializer extends CanDeserializeJournal[BSONDocument] {

    override def deserializeDocument(document: BSONDocument)(implicit serialization: Serialization, system: ActorSystem): Event = document match {
      case Version(1,doc) => deserializeVersionOne(doc)
      case Version(0,doc) => deserializeDocumentLegacy(doc)
      case Version(x,_) => throw new IllegalStateException(s"Don't know how to deserialize version $x of document")
      case _ => throw new IllegalStateException("Failed to read or default version field")
    }

    private def deserializeVersionOne(d: BSONDocument)(implicit serialization: Serialization, system: ActorSystem): Event =
      Event(
        pid = d.as[String](PROCESSOR_ID),
        sn = d.as[Long](SEQUENCE_NUMBER),
        payload = deserializePayload(d.get(PayloadKey).get,d.as[String](TYPE),d.getAs[String](HINT)),
        sender = d.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption),
        manifest = d.getAs[String](MANIFEST)
      )

    private def deserializePayload(b: BSONValue, clue: String, clazzName: Option[String])(implicit serialization: Serialization): Payload = (clue,b) match {
      case ("ser",BSONBinary(bfr, _)) if clazzName.isDefined =>
        val clazz = Class.forName(clazzName.get)
        Serialized(bfr.readArray(bfr.size),clazz)
      case ("bson",d:BSONDocument) => Bson(d)
      case ("bin",BSONBinary(bfr, _)) => Bin(bfr.readArray(bfr.size))
      case ("s",BSONString(s)) => StringPayload(s)
      case ("d",BSONDouble(d)) => DoublePayload(d)
      case ("f",BSONDouble(d)) => FloatPayload(d.toFloat)
      case ("l",BSONLong(l)) => LongPayload(l)
      case ("i",BSONInteger(i)) => IntPayload(i)
      case ("sh",BSONInteger(i)) => ShortPayload(i.toShort)
      case ("by",BSONInteger(i)) => BytePayload(i.toByte)
      case ("c",BSONInteger(i)) => CharPayload(i.toChar)
      case ("b",BSONBoolean(bln)) => BooleanPayload(bln)
      case (x,y) => throw new IllegalArgumentException(s"Unknown hint $x or type for payload content $y")
    }


    private def deserializeDocumentLegacy(document: BSONDocument)(implicit serialization: Serialization, system: ActorSystem): Event = {
      document.get(SERIALIZED) match {
        case Some(b: BSONDocument) =>
          Event(pid = document.as[String](PROCESSOR_ID),
                sn = document.as[Long](SEQUENCE_NUMBER),
                payload = Bson(b.as[BSONDocument](PayloadKey)),
                sender = b.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption),
                manifest = None)
        case Some(ser: BSONBinary) =>
          val repr = serialization.deserialize(ser.byteArray, classOf[PersistentRepr])
            .getOrElse(throw new IllegalStateException("Unable to deserialize PersistentRepr"))
          Event[BSONDocument](repr)
        case Some(x) =>
          throw new IllegalStateException(s"Unexpected value $x for $SERIALIZED field in document")
        case None =>
          throw new IllegalStateException(s"Cannot find required field $SERIALIZED in document")
      }
    }
  }

  implicit object JournalSerializer extends CanSerializeJournal[BSONDocument] {

    override def serializeAtom(atoms: TraversableOnce[Atom])(implicit serialization: Serialization, system: ActorSystem): BSONDocument = {
      val serd = atoms.map( atom =>
        BSONDocument(
          PROCESSOR_ID -> atom.pid,
          FROM -> atom.from,
          TO -> atom.to,
          EVENTS -> BSONArray(atom.events.map(serializeEvent))
        )
      )
      BSONDocument(ATOM -> BSONArray(serd.toSeq), VERSION -> 1)
    }

    import Producer._
    private def serializeEvent(event: Event)(implicit serialization: Serialization, system: ActorSystem): BSONDocument = {
      val doc = serializePayload(event.payload)(
        BSONDocument(VERSION -> 1, PROCESSOR_ID -> event.pid, SEQUENCE_NUMBER -> event.sn))
      (for {
        d <- Option(doc)
        d <- event.manifest.map(m => d.add(MANIFEST -> m)).orElse(Option(d))
        d <- event.sender
                  .filterNot(_ == system.deadLetters)
                  .flatMap(serialization.serialize(_).toOption)
                  .map(BSON.write(_))
                  .map(b => d.add(SenderKey -> b)).orElse(Option(d))
      } yield d).getOrElse(doc)
    }

    private def serializePayload(payload: Payload)(document: BSONDocument) = {
      val asDoc = payload match {
        case Bson(doc: BSONDocument) => BSONDocument(PayloadKey -> doc)
        case Bin(bytes) => BSONDocument(PayloadKey -> bytes)
        case s: Serialized[_] =>
          BSONDocument(PayloadKey -> BSON.write(s.bytes),
                       HINT -> s.clazz.getName)
        case StringPayload(str) => BSONDocument(PayloadKey -> str)
        case DoublePayload(dbl) => BSONDocument(PayloadKey -> dbl)
        case FloatPayload(f) => BSONDocument(PayloadKey -> BSONDouble(f))
        case IntPayload(i) => BSONDocument(PayloadKey -> i)
        case LongPayload(l) => BSONDocument(PayloadKey -> l)
        case ShortPayload(s) => BSONDocument(PayloadKey -> BSONInteger(s))
        case BytePayload(by) => BSONDocument(PayloadKey -> BSONInteger(by))
        case CharPayload(c) => BSONDocument(PayloadKey -> BSONInteger(c))
        case BooleanPayload(bl) => BSONDocument(PayloadKey -> bl)
        case x => throw new IllegalArgumentException(s"Unable to serialize payload of type $x")
      }

      document.add(BSONDocument(TYPE -> payload.hint).add(asDoc))
    }
  }

}
