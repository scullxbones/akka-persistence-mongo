package akka.contrib.persistence.mongodb

import com.mongodb.DBObject
import org.bson.BSONObject
import org.scalatest.matchers.{MatchResult, Matcher}

trait MongoMatchers {

  class DBObjectContains[V](key: String, value: V) extends Matcher[DBObject] {
    def apply(left: DBObject) = {
      val contained = Option(left.get(key)).orNull
      MatchResult(
          value.equals(contained),
          s"DBObject $key did not match $value, received $contained",
          s"DBObject $key matched value $value"
      )
    }
  }
  
  def have[V](key: String, value: V) = new DBObjectContains(key,value)
  
}

object MongoMatchers extends MongoMatchers {
  
  implicit class RichBsonObject(obj: BSONObject) {
    def apply(key: String) = obj.get(key)
  }
  
}