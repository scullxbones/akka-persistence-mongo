package com.github.scullxbones.mongka

import akka.actor._
import akka.actor.mailbox._
import akka.dispatch._
import akka.pattern.CircuitBreaker
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits._
import com.typesafe.config.Config
import reactivemongo.bson._
import reactivemongo.core.commands._
import scala.concurrent.Await

class MongkaMailboxSettings(override val systemSettings: ActorSystem.Settings, override val userConfig: Config)
  extends UserOverrideSettings(systemSettings, userConfig) {

  protected val name = "mailbox"

  val WriteTimeout = Duration(config.getMilliseconds("timeout.write"), MILLISECONDS)
  val ReadTimeout = Duration(config.getMilliseconds("timeout.read"), MILLISECONDS)
}

class MongkaMailboxType(systemSettings: ActorSystem.Settings, config: Config)
  extends MailboxType {

  private[this] val settings = new MongkaMailboxSettings(systemSettings, config)

  override def create(owner: Option[ActorRef],
    system: Option[ActorSystem]): MessageQueue =
    (owner zip system) headOption match {
      case Some((o, s: ExtendedActorSystem)) ⇒ new MongkaMessageQueue(o, s, settings)
      case _ ⇒
        throw new IllegalArgumentException("requires an owner " +
          "(i.e. does not work with BalancingDispatcher)")
    }
}

class MongkaMessageQueue(owner: ActorRef, system: ExtendedActorSystem, settings: MongkaMailboxSettings)
  extends DurableMessageQueue(owner, system) with DurableMessageSerialization {

  import serializers._

  val extension = system.extension(MongkaExtensionId)

  val collection = extension.collection(name)

  val breaker = extension.breaker

  def enqueue(receiver: ActorRef, envelope: Envelope): Unit = Await.result(
    breaker.withCircuitBreaker(
      collection.insert(BSONDocument("env" -> serialize(envelope))).mapTo[Unit]), settings.WriteTimeout)

  def dequeue(): Envelope = Await.result(
    breaker.withCircuitBreaker {
      collection.find(BSONDocument()).one[BSONDocument]
        .map(_.get.getAs[Array[Byte]]("env").get)
    }.map(deserialize(_)), settings.ReadTimeout)

  def hasMessages: Boolean = numberOfMessages > 0

  def numberOfMessages: Int = Await.result(
    breaker.withCircuitBreaker {
      collection.db.command(new Count(collection.name))
    }, settings.ReadTimeout)

  /**
   * Called when the mailbox is disposed.
   * An ordinary mailbox would send remaining messages to deadLetters,
   * but the purpose of a durable mailbox is to continue
   * with the same message queue when the actor is started again.
   */
  def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = ()

}
