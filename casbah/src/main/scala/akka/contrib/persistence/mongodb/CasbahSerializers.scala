package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.PersistentRepr
import akka.serialization.Serialization
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._

object CasbahSerializers extends JournallingFieldNames {

  implicit val dt: DocumentType[DBObject] = new DocumentType[DBObject] { }

  object Version {
    def unapply(dbo: DBObject): Option[(Int,DBObject)] = {
      dbo.getAs[Int](VERSION).orElse(Option(0)).map(_ -> dbo)
    }
  }

  implicit object Deserializer extends CanDeserializeJournal[DBObject] {

    override def deserializeDocument(dbo: DBObject)(implicit serialization: Serialization, system: ActorSystem): Event = dbo match {
      case Version(1,d) => deserializeVersionOne(d)
      case Version(0,d) => deserializeDocumentLegacy(d)
      case Version(x,_) => throw new IllegalStateException(s"Don't know how to deserialize version $x of document")
    }

    private def deserializeVersionOne(d: DBObject)(implicit serialization: Serialization, system: ActorSystem) = Event(
      pid = d.as[String](PROCESSOR_ID),
      sn = d.as[Long](SEQUENCE_NUMBER),
      payload = Payload[DBObject](d.as[String](TYPE),d.as[Any](PayloadKey),d.getAs[String](HINT),d.getAs[String](SER_MANIFEST)),
      sender = d.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption),
      manifest = d.getAs[String](MANIFEST),
      writerUuid = d.getAs[String](WRITER_UUID)
    )

    private def deserializeDocumentLegacy(d: DBObject)(implicit serialization: Serialization, system: ActorSystem) = {
      val persistenceId = d.as[String](PROCESSOR_ID)
      val sequenceNr = d.as[Long](SEQUENCE_NUMBER)
      d.get(SERIALIZED) match {
        case b: DBObject =>
          Event(
            pid = persistenceId,
            sn = sequenceNr,
            payload = Bson(b.as[DBObject](PayloadKey)),
            sender = b.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption),
            manifest = None,
            writerUuid = None
          )
        case _ =>
          val content = d.as[Array[Byte]](SERIALIZED)
          val repr = Serialized(content, classOf[PersistentRepr], None)
          Event[DBObject](repr.content).copy(pid = persistenceId, sn = sequenceNr)
//            pid = persistenceId,
//            sn = sequenceNr,
//            payload = repr,
//            sender = Option(repr.content.sender),
//            manifest = None,
//            writerUuid = None
//          )
      }

    }

  }

  implicit object Serializer extends CanSerializeJournal[DBObject] {
    override def serializeAtom(atom: Atom)(implicit serialization: Serialization, system: ActorSystem): DBObject = {
      MongoDBObject(
        PROCESSOR_ID -> atom.pid,
        FROM -> atom.from,
        TO -> atom.to,
        EVENTS -> MongoDBList(atom.events.map(serializeEvent): _*),
        VERSION -> 1
      )
    }

    private def serializeEvent(event: Event)(implicit serialization: Serialization, system: ActorSystem): DBObject = {
      val b = serializePayload(event.payload)(MongoDBObject.newBuilder ++= (
          VERSION -> 1 ::
            PROCESSOR_ID -> event.pid ::
            SEQUENCE_NUMBER -> event.sn ::
            Nil
        ))
      (for {
        bldr <- Option(b)
        bldr <- event.manifest.map(s => bldr += (MANIFEST -> s)).orElse(Option(bldr))
        bldr <- event.writerUuid.map(s => bldr += (WRITER_UUID -> s)).orElse(Option(bldr))
        bldr <- event.sender
          .filterNot(_ == system.deadLetters)
          .flatMap(serialization.serialize(_).toOption)
          .map(s => bldr += (SenderKey -> s)).orElse(Option(bldr))
      } yield bldr).getOrElse(b).result()
    }

    private def serializePayload(payload: Payload)(b: collection.mutable.Builder[(String,Any),DBObject]) = {
      val builder = b += (TYPE -> payload.hint)
      payload match {
        case Bson(doc: DBObject) => builder += PayloadKey -> doc
        case Bin(bytes) => builder += PayloadKey -> bytes
        case s: Serialized[_] => builder ++= (PayloadKey -> s.bytes :: HINT -> s.clazz.getName :: SER_MANIFEST -> s.serializedManifest :: Nil)
        case StringPayload(str) => builder += PayloadKey -> str
        case FloatingPointPayload(d) => builder += PayloadKey -> d
        case FixedPointPayload(l) => builder += PayloadKey -> l
        case BooleanPayload(bl) => builder += PayloadKey -> bl
      }
    }
  }
}
