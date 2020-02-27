package org.github.ruleProcessor

case class State(outputTag:String,asset:String,timestamp:Long,parameters:Map[String,Double],expression:String,uniqueParametersCount:Int,ruleWaitTime:Int,ruleFrequency:Int)
