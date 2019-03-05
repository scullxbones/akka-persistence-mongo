package akka.contrib.persistence.mongodb

import com.mongodb.MongoCommandException

object MongoErrors {

  object NamespaceExists extends CommandExceptionErrorCode(48)

}

abstract class CommandExceptionErrorCode(expectedErrorCode: Int) {

  def unapply(scrutinee: MongoCommandException): Boolean =
    expectedErrorCode == scrutinee.getErrorCode
}
