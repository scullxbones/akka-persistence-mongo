package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.PersistentRepr
import akka.serialization.Serialization
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import collection.immutable.{Seq => ISeq}

object CasbahSerializers {
  import JournallingFieldNames._

  object Version {
    def unapply(dbo: DBObject): Option[(Int,DBObject)] = {
      dbo.getAs[Int](VERSION).orElse(Option(0)).map(_ -> dbo)
    }
  }

  implicit object V1Deser extends CanDeserialize[DBObject] {

    override def deserializeRepr(implicit serialization: Serialization, system: ActorSystem) = {
      case Version(1,dbo) =>
        PersistentRepr(
          payload = None,
          persistenceId = dbo.as[String](PROCESSOR_ID),
          sequenceNr = dbo.as[Long](SEQUENCE_NUMBER),
          sender = dbo.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption).getOrElse(system.deadLetters),
          manifest = dbo.getAs[String](MANIFEST).getOrElse(PersistentRepr.Undefined)
        ).withPayload {
          dbo.as[String](TYPE) match {
            case "bson" => dbo.as[DBObject](PayloadKey)
            case "bin" => dbo.as[Array[Byte]](PayloadKey)
            case "s" => dbo.as[String](PayloadKey)
            case "d" => dbo.as[Double](PayloadKey)
            case "f" => dbo.as[Float](PayloadKey)
            case "l" => dbo.as[Long](PayloadKey)
            case "i" => dbo.as[Int](PayloadKey)
            case "sh" => dbo.as[Short](PayloadKey)
            case "by" => dbo.as[Byte](PayloadKey)
            case "c" => dbo.as[Char](PayloadKey)
            case "b" => dbo.as[Boolean](PayloadKey)
            case "ser" =>
              val hint = dbo.as[String](HINT)
              dbo.getAs[Array[Byte]](PayloadKey).flatMap(serialization.deserialize(_, Class.forName(hint)).toOption)
          }
        }
      case Version(0,dbo) => deserializeReprLegacy(dbo)
    }
  }

  def deserializeReprLegacy(document: DBObject)(implicit serialization: Serialization, system: ActorSystem): PersistentRepr = {
    document.get(SERIALIZED) match {
      case b: DBObject =>
        PersistentRepr.apply(
          payload = b.as[DBObject](PayloadKey),
          sequenceNr = document.as[Long](SEQUENCE_NUMBER),
          persistenceId = document.as[String](PROCESSOR_ID),
          deleted = document.as[Boolean](DELETED),
          sender = b.getAs[Array[Byte]](SenderKey).flatMap(serialization.deserialize(_, classOf[ActorRef]).toOption).getOrElse(system.deadLetters)
        )
      case _ =>
        val content = document.as[Array[Byte]](SERIALIZED)
        val repr = serialization.deserialize(content, classOf[PersistentRepr]).get
        PersistentRepr(
          payload = repr.payload,
          sequenceNr = document.as[Long](SEQUENCE_NUMBER),
          persistenceId = document.as[String](PROCESSOR_ID),
          deleted = document.as[Boolean](DELETED),
          sender = repr.sender)
    }
  }

  implicit object V1Ser extends CanSerialize[DBObject] {
    override def serializeAtomic(payload: ISeq[PersistentRepr])(implicit serialization: Serialization, system: ActorSystem): DBObject = {
      val serialized = payload.groupBy(_.persistenceId).map { case (persistenceId, batch) =>
        val (minSeq, maxSeq) = batch.foldLeft((Long.MaxValue, 0L)) { case ((min, max), repr) =>
          (if (repr.sequenceNr < min) repr.sequenceNr else min, if (repr.sequenceNr > max) repr.sequenceNr else max)
        }
        MongoDBObject(
          PROCESSOR_ID -> persistenceId,
          FROM -> minSeq,
          TO -> maxSeq,
          EVENTS -> MongoDBList(batch.map(serializeRepr): _*)
        )
      }
      MongoDBObject(ATOM -> serialized, VERSION -> 1)
    }

    override def serializeRepr(repr: PersistentRepr)(implicit serialization: Serialization, system: ActorSystem): DBObject = {
      val builder = MongoDBObject.newBuilder ++= (
        VERSION -> 1 ::
          PROCESSOR_ID -> repr.persistenceId ::
          SEQUENCE_NUMBER -> repr.sequenceNr ::
          SenderKey -> Option(repr.sender).filterNot(_ == system.deadLetters).flatMap(serialization.serialize(_).toOption) ::
          MANIFEST -> repr.manifest :: Nil
        )

      repr.payload match {
        case dbo: DBObject => builder ++= PayloadKey -> dbo :: TYPE -> "bson" :: Nil
        case bin: Array[Byte] => builder ++= PayloadKey -> bin :: TYPE -> "bin" :: Nil
        case s: String => builder ++= PayloadKey -> s :: TYPE -> "s" :: Nil
        case d: Double => builder ++= PayloadKey -> d :: TYPE -> "d" :: Nil
        case f: Float => builder ++= PayloadKey -> f :: TYPE -> "f" :: Nil
        case l: Long => builder ++= PayloadKey -> l :: TYPE -> "l" :: Nil
        case i: Int => builder ++= PayloadKey -> i :: TYPE -> "i" :: Nil
        case s: Short => builder ++= PayloadKey -> s :: TYPE -> "sh" :: Nil
        case b: Byte => builder ++= PayloadKey -> b :: TYPE -> "by" :: Nil
        case c: Char => builder ++= PayloadKey -> c :: TYPE -> "c" :: Nil
        case b: Boolean => builder ++= PayloadKey -> b :: TYPE -> "b" :: Nil
        case x: AnyRef => builder ++=
          PayloadKey -> serialization.serializerFor(x.getClass).toBinary(x) ::
            TYPE -> "ser" ::
            HINT -> x.getClass.getName ::
            Nil
      }

      builder.result()
    }
  }
}
