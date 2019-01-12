package akka.contrib.persistence.mongodb

import akka.actor.Props
import akka.contrib.persistence.mongodb.ConfigLoanFixture.withConfig
import akka.persistence.PersistentActor
import akka.persistence.query.PersistenceQuery
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit._
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.mongodb.scala.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ScalaDriverBsonPayloadSpec extends BaseUnitTest with ContainerMongo with BeforeAndAfterAll with ScalaFutures {

  private val bsonConfig: Config = ConfigFactory.parseString(
    s"""
       |akka.contrib.persistence.mongodb.mongo {
       |  mongouri = "mongodb://$host:$noAuthPort/$embedDB"
       |  db = "$embedDB"
       |}
       |
       |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
       |akka-contrib-mongodb-persistence-journal {
       |    # Class name of the plugin.
       |  class = "akka.contrib.persistence.mongodb.MongoJournal"
       |}
       |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
       |akka-contrib-mongodb-persistence-snapshot {
       |    # Class name of the plugin.
       |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
       |}
       |akka-contrib-mongodb-persistence-readjournal {
       |  # Class name of the plugin.
       |  class = "akka.contrib.persistence.mongodb.MongoReadJournal"
       |}
       |""".stripMargin).withFallback(ConfigFactory.defaultReference())

  override def embedDB = "official-scala-bson"

  override def beforeAll() = cleanup()

  private val documents = {
    val msg1 = BsonDocument("a" -> BsonInt32(1), "b" -> BsonString("2"))
    val msg2 = BsonDocument("a" -> BsonInt32(2), "b" -> BsonString("3"))
    msg1 :: msg2 :: Nil
  }

  "An official scala driver" should "support storage of `BsonDocument`s" in withConfig(bsonConfig,"akka-contrib-mongodb-persistence-journal","scala-payload-config") { case (actorSystem, _) =>
    implicit val as = actorSystem
    val underTest = actorSystem.actorOf(PayloadSpec.props("documents"))
    val probe = TestProbe()
    probe.send(underTest, PayloadSpec.Command(documents.head))
    probe.send(underTest, PayloadSpec.Command(documents(1)))
    probe.send(underTest, PayloadSpec.Get)
    val contents = probe.expectMsgType[PayloadSpec.Contents]
    contents.payload should contain theSameElementsInOrderAs documents.reverse
  }

  it should "support reading `BsonDocument` contents with read journal" in withConfig(bsonConfig,"akka-contrib-mongodb-persistence-journal","scala-payload-config") { case (actorSystem, _) =>
    implicit val as = actorSystem
    implicit val mat: Materializer = ActorMaterializer()
    val readJournal =
      PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
    val fut = readJournal.currentEventsByPersistenceId("documents", 0, Long.MaxValue)
      .runWith(Sink.seq)
    fut.futureValue(timeout(3.seconds.dilated)).map(_.event) should contain theSameElementsInOrderAs documents
  }

  private val arrays = {
    val msg1 = BsonArray(BsonInt32(1) :: BsonString("2") :: Nil)
    val msg2 = BsonArray(BsonDocument("a" -> BsonInt32(2)) :: BsonDocument("b" -> BsonString("3")) :: Nil)
    msg1 :: msg2 :: Nil
  }

  it should "support storage of `BsonArray`s" in withConfig(bsonConfig,"akka-contrib-mongodb-persistence-journal","scala-payload-config") { case (actorSystem, _) =>
    implicit val as = actorSystem
    val underTest = actorSystem.actorOf(PayloadSpec.props("arrays"))
    val probe = TestProbe()
    probe.send(underTest, PayloadSpec.Command(arrays.head))
    probe.send(underTest, PayloadSpec.Command(arrays(1)))
    probe.send(underTest, PayloadSpec.Get)
    val contents = probe.expectMsgType[PayloadSpec.Contents]
    contents.payload should contain theSameElementsInOrderAs arrays.reverse
  }

  it should "support reading `BsonArray` contents with read journal" in withConfig(bsonConfig,"akka-contrib-mongodb-persistence-journal","scala-payload-config") { case (actorSystem, _) =>
    implicit val as = actorSystem
    implicit val mat: Materializer = ActorMaterializer()
    val readJournal =
      PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
    val fut = readJournal.currentEventsByPersistenceId("arrays", 0, Long.MaxValue)
      .runWith(Sink.seq)
    fut.futureValue(timeout(3.seconds.dilated)).map(_.event) should contain theSameElementsInOrderAs arrays
  }

}

object PayloadSpec {
  def props(persistenceId: String): Props =
    Props(new BsonPayloadActor(persistenceId))

  case class Command(payload: BsonValue)
  case object Get
  case class Contents(payload: List[BsonValue])

  class BsonPayloadActor(val persistenceId: String) extends PersistentActor {

    private var state = List.empty[BsonValue]

    override def receiveRecover: Receive = {
      case bson:BsonValue =>
        state ::= bson
    }

    override def receiveCommand: Receive = {
      case Command(bson) =>
        persist(bson)(ev => state ::= ev)
      case Get =>
        sender() ! Contents(state)
    }
  }
}