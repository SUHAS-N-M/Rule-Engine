package org.github.ruleSource

case class Rule(deviceIdentifier:String,ruleExpression:String,parameters:String,outputTag:String,uniqueParametersCount:Int,ruleWaitTime:Int,ruleFrequency:Int)