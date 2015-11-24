package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.serialization.{SerializationExtension, Serialization}
import com.mongodb.util.JSON
import com.mongodb._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import collection.JavaConverters._
import scala.util.Try

abstract class JournalUpgradeSpec[D <: MongoPersistenceDriver, X <: MongoPersistenceExtension](extensionClass: Class[X], toDriver: (ActorSystem,Config) => D) extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  import ConfigLoanFixture._

  override def embedDB = "upgrade-test"

  override def beforeAll(): Unit = {
    doBefore()
  }

  override def afterAll(): Unit = {
    doAfter()
  }

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.journal-automatic-upgrade = true
    |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
    |akka-contrib-mongodb-persistence-journal {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoJournal"
    |  overrides {
    |     mongouri = "mongodb://localhost:$embedConnectionPort/$embedDB"
    |     database = $embedDB
    |  }
    |}
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka-contrib-mongodb-persistence-snapshot {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    |""".stripMargin)

  def configured[A](testCode: D => A) = withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "upgrade-test")(toDriver.tupled andThen testCode)

  "A mongo persistence driver" should "do nothing on a new installation" in configured { as =>
    mongoClient.getDB(embedDB).getCollectionNames shouldNot contain ("akka_persistence_journal")
  }

  import JournallingFieldNames._
  def buildLegacyObject[A](pid: String, sn: Long, payload: A)(implicit serEv: Serialization): DBObject = {
    val builder = new BasicDBObjectBuilder()
    builder
      .add(PROCESSOR_ID, pid)
      .add(SEQUENCE_NUMBER, sn)
      .add(SERIALIZED,
        serEv.serialize(PersistentRepr(payload, sn, pid)).get
      ).get()
  }

  def buildLegacyDocument[A](pid: String, sn: Long)(implicit serEv: Serialization): DBObject = {
    val builder = new BasicDBObjectBuilder()
    val serBuilder = new BasicDBObjectBuilder()
    val plBuilder = new BasicDBObjectBuilder()
    val subdoc = serBuilder.add(PayloadKey, plBuilder.add("abc",1).add("def",2.0).add("ghi",true).get()).get()
    builder.add(PROCESSOR_ID, pid).add(SEQUENCE_NUMBER, sn).add(SERIALIZED, subdoc).get()
  }

  def queryByProcessorId(pid: String): DBObject = {
    new BasicDBObjectBuilder().add(PROCESSOR_ID,pid).get()
  }

  def createLegacyIndex(coll: DBCollection): Unit = {
    val idxSpec =
      new BasicDBObjectBuilder()
        .add(PROCESSOR_ID, 1)
        .add(SEQUENCE_NUMBER, 1)
        .add(DELETED, 1)
        .get()

    Try(coll.createIndex(idxSpec)).getOrElse(())
  }

  it should "upgrade an existing journal" in configured { as =>
    implicit val serialization = SerializationExtension.get(as.actorSystem)
    val coll = mongoClient.getDB(embedDB).getCollection("akka_persistence_journal")

    createLegacyIndex(coll)
    coll.insert(buildLegacyObject("foo",1,"bar"))
    coll.insert(buildLegacyObject("foo",2,"bar"))
    coll.insert(buildLegacyDocument("foo",3))

    println(s"before = ${coll.find().toArray.asScala.toList}")

    as.upgradeJournalIfNeeded()

    val records = coll.find(queryByProcessorId("foo")).toArray.asScala.toList
    println(records)
    records should have size 3
    records.zipWithIndex.foreach { case (dbo,idx) =>
      dbo.get(PROCESSOR_ID) should be ("foo")
      dbo.get(TO) should be (idx + 1)
      dbo.get(FROM) should be (dbo.get(TO))
      val event = dbo.get(EVENTS).asInstanceOf[BasicDBList].get(0).asInstanceOf[DBObject]
      event.get(SEQUENCE_NUMBER) should be (idx + 1)
      if (idx < 2) {
        event.get(TYPE) should be ("s")
        event.get(PayloadKey) should be ("bar")
      } else {
        event.get(TYPE) should be ("bson")
        val bson = event.get(PayloadKey).asInstanceOf[DBObject]
        bson.get("abc") should be (1)
        bson.get("def") should be (2.0)
        bson.get("ghi") shouldBe true
      }
    }
  }

  it should "upgrade a more complicated journal" in configured { as =>
    implicit val serialization = SerializationExtension.get(as.actorSystem)
    val coll = mongoClient.getDB(embedDB).getCollection("akka_persistence_journal")
    coll.remove(new BasicDBObject())
    createLegacyIndex(coll)

    val doc =
    """
    |{
    |   "_id" : { "$oid" : "55deeae33de20e69f33b748b" },
    |   "pid" : "foo",
    |   "sn" : { "$numberLong" : "1" },
    |   "dl" : false,
    |   "cs" : [ ],
    |   "pr" : {
    |       "p" : {
    |           "order-created" : {
    |               "id" : "alsonotarealguid",
    |               "seqNr" : 232,
    |               "userId" : "notarealguid",
    |               "cartId" : "notarealcartid",
    |               "phoneNumber" : "+15555005555",
    |               "from" : {
    |                   "country" : "US"
    |               },
    |               "to" : {
    |                   "country" : "RU",
    |                   "region" : "MOW",
    |                   "city" : "Moscow"
    |               },
    |               "dateCreated" : { "$date": "2015-08-27T10:48:03.101Z" },
    |               "timestamp" : { "$date": "2015-08-27T10:48:03.101Z" },
    |               "addressId" : "not-a-real-addressid"
    |           },
    |           "_timestamp" : { "$date": "2015-08-27T10:48:03.102Z" }
    |       }
    |   }
    }""".stripMargin
    coll.insert(JSON.parse(doc).asInstanceOf[DBObject])

    as.upgradeJournalIfNeeded()

    val records = coll.find(queryByProcessorId("foo")).toArray.asScala.toList
    println(records)
    records should have size 1
    records.zipWithIndex.foreach { case (dbo,idx) =>
      dbo.get(PROCESSOR_ID) should be ("foo")
      dbo.get(TO) should be (idx + 1)
      dbo.get(FROM) should be (dbo.get(TO))
      val event = dbo.get(EVENTS).asInstanceOf[BasicDBList].get(0).asInstanceOf[DBObject]
      event.get(SEQUENCE_NUMBER) should be (idx + 1)
      event.get(TYPE) should be ("bson")
      val bson = event.get(PayloadKey).asInstanceOf[DBObject]
      val payload = bson.get("order-created").asInstanceOf[DBObject]
      payload.get("cartId") should be ("notarealcartid")
      payload.get("seqNr") should be (232)
    }

  }
}
