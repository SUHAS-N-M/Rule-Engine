package org.github.kafkaSink

import scala.collection.immutable.HashMap

case class WindowRuleState(asset:String,outputTag:String,timestamp:Long,values:HashMap[Long,Double],computedValue:Double)
