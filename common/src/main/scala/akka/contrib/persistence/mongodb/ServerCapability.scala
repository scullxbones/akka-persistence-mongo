package akka.contrib.persistence.mongodb

sealed trait ServerCapability
object ServerCapability {
  final case object CollectionChangeStream extends ServerCapability
  final case object DatabaseChangeStream extends ServerCapability
  final case object MultiDocumentTransaction extends ServerCapability
}

sealed trait ServerCapabilitySet[C <: ServerVersion] {
  def supports(head: ServerCapability, tail: ServerCapability*): Boolean
}
object ServerCapabilitySet {
  def apply[C <: ServerVersion](implicit set: ServerCapabilitySet[C]): ServerCapabilitySet[C] = set
}