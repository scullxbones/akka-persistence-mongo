package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, ActorSystem, DynamicAccess, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.PersistentRepr
import akka.serialization.{Serialization, SerializationExtension}
import org.mongodb.scala._
import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.immutable.Document

import scala.collection.JavaConverters._

object ScalaDriverSerializersExtension extends ExtensionId[ScalaDriverSerializers] with ExtensionIdProvider {
  override def lookup: ExtensionId[ScalaDriverSerializers] = ScalaDriverSerializersExtension

  override def createExtension(system: ExtendedActorSystem): ScalaDriverSerializers =
    new ScalaDriverSerializers(system.dynamicAccess, system)

  override def get(system: ActorSystem): ScalaDriverSerializers = super.get(system)
}

class ScalaDriverSerializers(dynamicAccess: DynamicAccess, actorSystem: ActorSystem) extends Extension with JournallingFieldNames {

  implicit val serialization: Serialization = SerializationExtension(actorSystem)
  private implicit val system: ActorSystem = actorSystem
  implicit val loader: LoadClass = dynamicAccess

  implicit val dt: DocumentType[Document] = new DocumentType[Document] { }

  object Version {
    def unapply(doc: Document): Option[(Int,Document)] = {
      doc.get[bson.BsonInt32](VERSION).map(_.intValue()).orElse(Option(0)).map(_ -> doc)
    }
  }


  implicit object Deserializer extends CanDeserializeJournal[Document] {
    override def deserializeDocument(dbo: Document): Event = dbo match {
      case Version(1,d) => deserializeVersionOne(d)
      case Version(0,d) => deserializeDocumentLegacy(d)
      case Version(x,_) => throw new IllegalStateException(s"Don't know how to deserialize version $x of document")
    }

    private def extractTags(d: Document): Seq[String] =
      d.get[bson.BsonArray](TAGS)
        .map(_.getValues.asScala.collect{ case s:bson.BsonString => s.getValue })
        .getOrElse(Seq.empty[String])

    private def extractSender(d: Document): Option[ActorRef] =
      d.get[bson.BsonBinary](SenderKey).map(_.getData).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption)

    private def extractPayloadContent: PartialFunction[bson.BsonValue, Any] = {
      case bin: bson.BsonBinary => bin.getData
      case doc: bson.BsonDocument => Document(doc)
      case s: bson.BsonString => s.getValue
      case dbl: bson.BsonDouble => dbl.doubleValue()
      case dbl: bson.BsonDecimal128 => dbl.doubleValue()
      case lng: bson.BsonInt64 => lng.getValue
      case bl: bson.BsonBoolean => bl.getValue
    }

    private def deserializeVersionOne(d: Document) = Event(
      pid = d.getString(PROCESSOR_ID),
      sn = d.getLong(SEQUENCE_NUMBER),
      payload = Payload[Document](
        hint = d.getString(TYPE),
        any = d.get[bson.BsonValue](PayloadKey).collect(extractPayloadContent).get,
        tags = Set.empty[String] ++ extractTags(d),
        clazzName = d.get[BsonString](HINT).map(_.getValue),
        serId = d.get[BsonInt32](SER_ID).map(_.getValue),
        serManifest = d.get[BsonString](SER_MANIFEST).map(_.getValue)
      ),
      sender = extractSender(d),
      manifest = d.get[BsonString](MANIFEST).map(_.getValue),
      writerUuid = d.get[BsonString](WRITER_UUID).map(_.getValue)
    )

    private def deserializeDocumentLegacy(d: Document) = {
      val persistenceId = d.getString(PROCESSOR_ID)
      val sequenceNr = d.getLong(SEQUENCE_NUMBER)
      d.get[BsonDocument](SERIALIZED) match {
        case Some(b: BsonDocument) =>
          Event(
            pid = persistenceId,
            sn = sequenceNr,
            payload = Bson(Document(b.getDocument(PayloadKey).asScala), Set()),
            sender = extractSender(b),
            manifest = None,
            writerUuid = None
          )
        case _ =>
          val repr = (for {
            content <- d.get[bson.BsonBinary](SERIALIZED)
            buf = content.getData
            pr <- serialization.deserialize(buf, classOf[PersistentRepr]).toOption
          } yield pr).get
          Event[Document](useLegacySerialization = false)(repr).copy(pid = persistenceId, sn = sequenceNr)
      }

    }
  }

  implicit object Serializer extends CanSerializeJournal[Document] with DefaultBsonTransformers {
    override def serializeAtom(atom: Atom): Document = {
      Option(atom.tags).filter(_.nonEmpty).foldLeft(
        Document(
          PROCESSOR_ID -> atom.pid,
          FROM -> atom.from,
          TO -> atom.to,
          EVENTS -> atom.events.map(serializeEvent),
          VERSION -> 1
        )
      ){ case(o,tags) => o + (TAGS -> tags.toSeq)}
    }

    private def serializeEvent(event: Event): Document = {
      val b = serializePayload(event.payload)(Document(
          VERSION -> 1,
          PROCESSOR_ID -> event.pid,
          SEQUENCE_NUMBER -> event.sn,
          TAGS -> event.tags.toSeq
        ))
      (for {
        doc <- Option(b)
        doc <- Option(event.tags).filter(_.nonEmpty).map(tags => doc + (TAGS -> tags.toSeq)).orElse(Option(doc))
        doc <- event.manifest.map(s => doc + (MANIFEST -> s)).orElse(Option(doc))
        doc <- event.writerUuid.map(s => doc + (WRITER_UUID -> s)).orElse(Option(doc))
        doc <- event.sender
          .filterNot(_ == system.deadLetters)
          .flatMap(serialization.serialize(_).toOption)
          .map(s => doc + (SenderKey -> s)).orElse(Option(doc))
      } yield doc).getOrElse(b)
    }

    private def serializePayload(payload: Payload)(doc: Document): Document = {
      val withType = doc + (TYPE -> payload.hint)
      payload match {
        case Bson(doc: Document, _) => withType + (PayloadKey -> doc)
        case Bin(bytes, _) => withType + (PayloadKey -> bytes)
        case Legacy(bytes, _) => withType + (PayloadKey -> bytes)
        case s: Serialized[_] =>
          withType +
            (PayloadKey -> s.bytes) +
            (HINT -> s.className) +
            (SER_MANIFEST -> s.serializedManifest) +
            (SER_ID -> s.serializerId)
        case StringPayload(str, _) => withType + (PayloadKey -> str)
        case FloatingPointPayload(d, _) => withType + (PayloadKey -> d)
        case FixedPointPayload(l, _) => withType + (PayloadKey -> l)
        case BooleanPayload(bl, _) => withType + (PayloadKey -> bl)
        case x => throw new IllegalArgumentException(s"Unable to serialize payload for unknown type ${x.getClass.getName} with hint ${payload.hint}")
      }
    }
  }
}
