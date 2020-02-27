package org.github.ruleProcessor

case class Computed(outputTag:String,asset:String,timestamp:Long,parameters:Map[String,Double],expression:String,uniqueParametersCount:Int,ruleWaitTime:Int,ruleFrequency:Int,readyFlag:Boolean,computedValue:Double)
