package akka.contrib.persistence.mongodb

import com.mongodb.MongoCommandException

object MongoErrors {

  object NamespaceExists extends CommandExceptionErrorCode(48)

}

abstract class CommandExceptionErrorCode(val code: Int) {

  def unapply(scrutinee: MongoCommandException): Boolean =
    code == scrutinee.getErrorCode
}
