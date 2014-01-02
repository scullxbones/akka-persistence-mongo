package com.github.scullxbones

import _root_.akka.actor.ActorSystem
import com.typesafe.config.Config

package object mongka {

  abstract class UserOverrideSettings(val systemSettings: ActorSystem.Settings, val userConfig: Config) {
  
    protected val name: String
    
    private def fullName: String = List(getClass().getPackage().getName(), name).mkString(".")
    
    lazy protected val config = initialize
  
	private def initialize: Config =
	    if (userConfig.hasPath(name))
	      userConfig.getConfig(name).withFallback(systemSettings.config.getConfig(fullName))
	    else 
	      systemSettings.config.getConfig(fullName)
  }
  
}