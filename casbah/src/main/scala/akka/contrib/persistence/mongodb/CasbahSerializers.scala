package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, ActorSystem, DynamicAccess, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.persistence.PersistentRepr
import akka.serialization.{Serialization, SerializationExtension}
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import org.bson.types.ObjectId

object CasbahSerializersExtension extends ExtensionId[CasbahSerializers] with ExtensionIdProvider {
  override def lookup = CasbahSerializersExtension

  override def createExtension(system: ExtendedActorSystem) =
    new CasbahSerializers(system.dynamicAccess, system)

  override def get(system: ActorSystem): CasbahSerializers = super.get(system)
}

class CasbahSerializers(dynamicAccess: DynamicAccess, actorSystem: ActorSystem) extends Extension with JournallingFieldNames {

  implicit val serialization: Serialization = SerializationExtension(actorSystem)
  private implicit val system = actorSystem
  implicit val loader: LoadClass = dynamicAccess

  implicit val dt: DocumentType[DBObject] = new DocumentType[DBObject] { }

  object Version {
    def unapply(dbo: DBObject): Option[(Int,DBObject)] = {
      dbo.getAs[Int](VERSION).orElse(Option(0)).map(_ -> dbo)
    }
  }

  implicit object Deserializer extends CanDeserializeJournal[DBObject] {

    override def deserializeDocument(dbo: DBObject): Event = dbo match {
      case Version(1,d) => deserializeVersionOne(d)
      case Version(0,d) => deserializeDocumentLegacy(d)
      case Version(x,_) => throw new IllegalStateException(s"Don't know how to deserialize version $x of document")
    }

    private def deserializeVersionOne(d: DBObject) = Event(
      pid = d.as[String](PROCESSOR_ID),
      sn = d.as[Long](SEQUENCE_NUMBER),
      payload = Payload[DBObject](
        hint = d.as[String](TYPE),
        any = d.as[Any](PayloadKey),
        tags = Set.empty[String] ++ d.getAs[MongoDBList](TAGS).toList.flatMap(_.collect{case s: String => s }),
        clazzName = d.getAs[String](HINT),
        serId = d.getAs[Int](SER_ID),
        serManifest = d.getAs[String](SER_MANIFEST)
      ),
      sender = d.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption),
      manifest = d.getAs[String](MANIFEST),
      writerUuid = d.getAs[String](WRITER_UUID)
    )

    private def deserializeDocumentLegacy(d: DBObject) = {
      val persistenceId = d.as[String](PROCESSOR_ID)
      val sequenceNr = d.as[Long](SEQUENCE_NUMBER)
      d.get(SERIALIZED) match {
        case b: DBObject =>
          Event(
            pid = persistenceId,
            sn = sequenceNr,
            payload = Bson(b.as[DBObject](PayloadKey), Set()),
            sender = b.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption),
            manifest = None,
            writerUuid = None
          )
        case _ =>
          val content = d.as[Array[Byte]](SERIALIZED)
          val repr = serialization.deserialize(content, classOf[PersistentRepr]).get
          Event[DBObject](useLegacySerialization = false)(repr).copy(pid = persistenceId, sn = sequenceNr)
      }

    }

  }

  implicit object Serializer extends CanSerializeJournal[DBObject] {
    override def serializeAtom(atom: Atom): DBObject = {
      Option(atom.tags).filter(_.nonEmpty).foldLeft(
        MongoDBObject(
          ID -> ObjectId.get(),
          PROCESSOR_ID -> atom.pid,
          FROM -> atom.from,
          TO -> atom.to,
          EVENTS -> MongoDBList(atom.events.map(serializeEvent): _*),
          VERSION -> 1
        )
      ){ case(o,tags) => o ++= MongoDBObject(TAGS -> serializeTags(tags))}
    }

    private def serializeEvent(event: Event): DBObject = {
      val b = serializePayload(event.payload)(MongoDBObject.newBuilder ++= (
          VERSION -> 1 ::
            PROCESSOR_ID -> event.pid ::
            SEQUENCE_NUMBER -> event.sn ::
            TAGS -> event.tags.foldLeft(MongoDBList.newBuilder[String])(_ += _).result() ::
            Nil
        ))
      (for {
        bldr <- Option(b)
        bldr <- Option(event.tags).filter(_.nonEmpty).map(tags => bldr += (TAGS -> serializeTags(tags))).orElse(Option(bldr))
        bldr <- event.manifest.map(s => bldr += (MANIFEST -> s)).orElse(Option(bldr))
        bldr <- event.writerUuid.map(s => bldr += (WRITER_UUID -> s)).orElse(Option(bldr))
        bldr <- event.sender
          .filterNot(_ == system.deadLetters)
          .flatMap(serialization.serialize(_).toOption)
          .map(s => bldr += (SenderKey -> s)).orElse(Option(bldr))
      } yield bldr).getOrElse(b).result()
    }

    private def serializeTags(tags: Set[String]): MongoDBList =
      tags.foldLeft(MongoDBList.newBuilder[String])(_ += _).result()

    private def serializePayload(payload: Payload)(b: collection.mutable.Builder[(String,Any),DBObject]) = {
      val builder = b += (TYPE -> payload.hint)
      payload match {
        case Bson(doc: DBObject, _) => builder += PayloadKey -> doc
        case Bin(bytes, _) => builder += PayloadKey -> bytes
        case Legacy(bytes, _) => builder += PayloadKey -> bytes
        case s: Serialized[_] => builder ++= (PayloadKey -> s.bytes :: HINT -> s.className :: SER_MANIFEST -> s.serializedManifest :: SER_ID -> s.serializerId :: Nil)
        case StringPayload(str, _) => builder += PayloadKey -> str
        case FloatingPointPayload(d, _) => builder += PayloadKey -> d
        case FixedPointPayload(l, _) => builder += PayloadKey -> l
        case BooleanPayload(bl, _) => builder += PayloadKey -> bl
        case x => throw new IllegalArgumentException(s"Unable to serialize payload for unknown type ${x.getClass.getName} with hint ${payload.hint}")
      }
    }
  }
}
