package org.github.kafkaSink

case class RuleState(asset:String,outputTag:String,currentState:Double,timestamp:Long,publishFlag:Boolean)
