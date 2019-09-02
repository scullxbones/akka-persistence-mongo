package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, ActorSystem, DynamicAccess, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.PersistentRepr
import akka.serialization.{Serialization, SerializationExtension}
import org.mongodb.scala._
import org.mongodb.scala.bson._

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

  implicit val dt: DocumentType[BsonValue] = new DocumentType[BsonValue] { }

  object Version {
    def unapply(doc: BsonDocument): Option[(Int,BsonDocument)] = {
      Option(doc.get(VERSION)).filter(_.isInt32).map(_.asInt32).map(_.intValue).orElse(Option(0)).map(_ -> doc)
    }
  }


  implicit object Deserializer extends CanDeserializeJournal[BsonDocument] {
    override def deserializeDocument(dbo: BsonDocument): Event = dbo match {
      case Version(1,d) => deserializeVersionOne(d.asDocument())
      case Version(0,d) => deserializeDocumentLegacy(d.asDocument())
      case Version(x,_) => throw new IllegalStateException(s"Don't know how to deserialize version $x of document")
    }

    private def extractTags(d: BsonDocument): scala.collection.Seq[String] =
      Option(d.get(TAGS)).filter(_.isArray).map(_.asArray)
        .map(_.getValues.asScala.collect{ case s:bson.BsonString => s.getValue })
        .getOrElse(Seq.empty[String])

    private def extractSender(d: BsonDocument): Option[ActorRef] =
      Option(d.get(SenderKey)).filter(_.isBinary).map(_.asBinary)
        .map(_.getData)
        .flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption)

    private def extractPayloadContent: PartialFunction[bson.BsonValue, Any] = {
      case bin: bson.BsonBinary => bin.getData
      case doc: bson.BsonDocument => doc
      case arr: bson.BsonArray => arr
      case s: bson.BsonString => s.getValue
      case dbl: bson.BsonDouble => dbl.doubleValue
      case dbl: bson.BsonDecimal128 => dbl.doubleValue
      case lng: bson.BsonInt64 => lng.getValue
      case int: bson.BsonInt32 => int.getValue
      case bl: bson.BsonBoolean => bl.getValue
    }

    private def deserializeVersionOne(d: BsonDocument) = Event(
      pid = d.getString(PROCESSOR_ID).getValue,
      sn = d.getLong(SEQUENCE_NUMBER),
      payload = Payload[BsonValue](
        hint = d.getString(TYPE).getValue,
        any = Option(d.get(PayloadKey)).collect(extractPayloadContent).get,
        tags = Set.empty[String] ++ extractTags(d),
        clazzName = Option(d.get(HINT)).filter(_.isString).map(_.asString).map(_.getValue),
        serId = Option(d.get(SER_ID)).filter(_.isInt32).map(_.asInt32).map(_.getValue),
        serManifest = Option(d.get(SER_MANIFEST)).filter(_.isString).map(_.asString).map(_.getValue)
      ),
      sender = extractSender(d),
      manifest = Option(d.get(MANIFEST)).filter(_.isString).map(_.asString).map(_.getValue),
      writerUuid = Option(d.get(WRITER_UUID)).filter(_.isString).map(_.asString).map(_.getValue)
    )

    private def deserializeDocumentLegacy(d: BsonDocument) = {
      val persistenceId = d.getString(PROCESSOR_ID).getValue
      val sequenceNr = d.getLong(SEQUENCE_NUMBER)
      Option(d.get(SERIALIZED)) match {
        case Some(b: BsonDocument) =>
          Event(
            pid = persistenceId,
            sn = sequenceNr,
            payload = Bson[BsonValue](b.get(PayloadKey), Set()),
            sender = extractSender(b),
            manifest = None,
            writerUuid = None
          )
        case _ =>
          val repr = (for {
            content <- Option(d.get(SERIALIZED)).filter(_.isBinary).map(_.asBinary)
            buf = content.getData
            pr <- serialization.deserialize(buf, classOf[PersistentRepr]).toOption
          } yield pr).get
          Event[BsonValue](useLegacySerialization = false)(repr).copy(pid = persistenceId, sn = sequenceNr)
      }

    }
  }

  implicit object Serializer extends CanSerializeJournal[BsonDocument] with DefaultBsonTransformers {
    override def serializeAtom(atom: Atom): BsonDocument = {
      Option(atom.tags).filter(_.nonEmpty).foldLeft(
        BsonDocument(
          ID -> BsonObjectId(),
          PROCESSOR_ID -> atom.pid,
          FROM -> atom.from,
          TO -> atom.to,
          EVENTS -> atom.events.map(serializeEvent),
          VERSION -> 1
        )
      ){ case(o,tags) => o.append(TAGS, serializeTags(tags))}
    }

    private def serializeEvent(event: Event): BsonDocument = {
      val b = serializePayload(event.payload)(BsonDocument(
          VERSION -> 1,
          PROCESSOR_ID -> event.pid,
          SEQUENCE_NUMBER -> event.sn
        ))
      (for {
        doc <- Option(b)
        doc <- Option(event.tags).filter(_.nonEmpty).map(tags => doc.append(TAGS, serializeTags(tags))).orElse(Option(doc))
        doc <- event.manifest.map(s => doc.append(MANIFEST, BsonString(s))).orElse(Option(doc))
        doc <- event.writerUuid.map(s => doc.append(WRITER_UUID, BsonString(s))).orElse(Option(doc))
        doc <- event.sender
          .filterNot(_ == system.deadLetters)
          .flatMap(serialization.serialize(_).toOption)
          .map(s => doc.append(SenderKey, BsonBinary(s))).orElse(Option(doc))
      } yield doc).getOrElse(b)
    }

    private def serializeTags(tags: Set[String]): BsonArray =
      BsonArray.fromIterable(tags.map(BsonString(_)))

    private def serializePayload(payload: Payload)(doc: BsonDocument): BsonDocument = {
      val withType = doc.append(TYPE, BsonString(payload.hint))
      payload match {
        case Bson(doc: BsonValue, _) => withType.append(PayloadKey, doc)
        case Bin(bytes, _) => withType.append(PayloadKey, BsonBinary(bytes))
        case Legacy(bytes, _) => withType.append(PayloadKey, BsonBinary(bytes))
        case s: Serialized[_] =>
          withType
            .append(PayloadKey, BsonBinary(s.bytes))
            .append(HINT, BsonString(s.className))
            .append(SER_MANIFEST, s.serializedManifest.map(BsonString(_)).getOrElse(BsonNull()))
            .append(SER_ID, s.serializerId.map(BsonInt32(_)).getOrElse(BsonNull()))
        case StringPayload(str, _) => withType.append(PayloadKey, BsonString(str))
        case FloatingPointPayload(d, _) => withType.append(PayloadKey, BsonDouble(d))
        case FixedPointPayload(l, _) => withType.append(PayloadKey, BsonInt64(l))
        case BooleanPayload(bl, _) => withType.append(PayloadKey, BsonBoolean(bl))
        case x => throw new IllegalArgumentException(s"Unable to serialize payload for unknown type ${x.getClass.getName} with hint ${payload.hint}")
      }
    }
  }
}
