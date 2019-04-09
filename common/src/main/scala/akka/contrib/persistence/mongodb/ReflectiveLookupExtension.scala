package akka.contrib.persistence.mongodb

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

import scala.reflect.ClassTag
import scala.util.Try

object ReflectiveLookupExtension extends ExtensionId[ReflectiveLookupExtension] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): ReflectiveLookupExtension =
    new ReflectiveLookupExtension(system)

  override def lookup(): ExtensionId[ReflectiveLookupExtension] = ReflectiveLookupExtension
}

class ReflectiveLookupExtension(extendedActorSystem: ExtendedActorSystem) extends Extension {
  def reflectClassByName[T:ClassTag](fqcn: String): Try[Class[_ <: T]] =
    extendedActorSystem.dynamicAccess.getClassFor[T](fqcn)

  def unsafeReflectClassByName[T:ClassTag](fqcn: String): Class[_ <: T] =
    reflectClassByName[T](fqcn).get
}