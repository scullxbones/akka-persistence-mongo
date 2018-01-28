/*
 * Copyright (c) 2018      Gael Breard, Orange: Fix issue #179 about actorRef serialization
 */
package akka.contrib.persistence.mongodb

import akka.actor.ExtendedActorSystem
import akka.serialization.Serialization

object SerializationHelper {
  //may be replaced with future API in Serialization : Serialization.withTransportInformation(ser.system)
  def withTransportInformation[T](system: ExtendedActorSystem)(f: => T): T = {
    val address = system.provider.getDefaultAddress
    if (address.hasLocalScope) {
      f
    } else {
      Serialization.currentTransportInformation.withValue(Serialization.Information(address, system)) {
        f
      }
    }
  }
}