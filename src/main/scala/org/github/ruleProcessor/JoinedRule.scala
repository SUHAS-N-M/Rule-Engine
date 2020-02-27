package org.github.ruleProcessor

case class JoinedRule(asset:String,tag:String,outputTag:String,ruleExpression:String,timestamp:Long,value:Double,uniqueParametersCount:Int,ruleWaitTime:Int,ruleFrequency:Int)
