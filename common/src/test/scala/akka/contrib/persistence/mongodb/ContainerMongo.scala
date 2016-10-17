package akka.contrib.persistence.mongodb

import com.mongodb._

trait ContainerMongo {
  def host = sys.env.getOrElse("CONTAINER_HOST","localhost")
  def authPort = 28117
  def noAuthPort = 27117
  def envMongoVersion = Option(sys.env.getOrElse("MONGODB_VERSION","3.2"))

  def embedDB: String = "akka_persist_mongo_test"
  def mongoClient =  new MongoClient(host,noAuthPort)

  def cleanup(dbName: String = embedDB): Unit = {
    println(s"Cleaning up db named $dbName")
    try {
      mongoClient.dropDatabase(dbName)
      mongoClient.close()
    } catch {
      case x: Throwable => x.printStackTrace()
    }
  }
}