package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.serialization.{SerializationExtension, Serialization}
import com.mongodb.{BasicDBList, BasicDBObjectBuilder, DBObject}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import collection.JavaConverters._

abstract class JournalUpgradeSpec[D <: MongoPersistenceDriver, X <: MongoPersistenceExtension](extensionClass: Class[X], toDriver: ActorSystem => D) extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  import ConfigLoanFixture._

  override def embedDB = "upgrade-test"

  override def beforeAll() {
    doBefore()
  }

  override def afterAll() {
    doAfter()
  }

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://localhost:$embedConnectionPort/$embedDB"
    |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
    |akka-contrib-mongodb-persistence-journal {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoJournal"
    |}
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka-contrib-mongodb-persistence-snapshot {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    |""".stripMargin)

  def configured[A](testCode: D => A) = withConfig(config(extensionClass), "upgrade-test")(toDriver andThen testCode)

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

  it should "upgrade an existing journal" in configured { as =>
    implicit val serialization = SerializationExtension.get(as.actorSystem)
    val coll = mongoClient.getDB(embedDB).getCollection("akka_persistence_journal")

    coll.insert(buildLegacyObject("foo",1,"bar"))
    coll.insert(buildLegacyObject("foo",2,"bar"))
    coll.insert(buildLegacyDocument("foo",3))

    println(s"before = ${coll.find().toArray().asScala.toList}")

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
}
