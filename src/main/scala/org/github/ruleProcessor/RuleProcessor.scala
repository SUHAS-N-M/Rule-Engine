package org.github.ruleProcessor

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.github.kafkaSource.Data
import org.github.ruleSource.{RawRule, Rule}

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object RuleProcessor {
  def splitParameters(rules:Dataset[RawRule]):Dataset[Rule] = {
    rules.flatMap(rule => {
      val x = rule.parameters
      for {
        a <- x.split(",")
      }yield Rule(rule.asset,rule.expression,a,rule.outputTag,rule.parameters.split(",").toSet.size,rule.ruleWaitTime.toInt,rule.ruleFrequency.toInt)
    })(Encoders.product[Rule])
  }

  def joinWithRules(kafkaStream:Dataset[Data],rules:Dataset[Rule]) :Dataset[JoinedRule]={
    kafkaStream.join(rules,(col("rules.deviceIdentifier") === col("kafkaStream.asset")).and(col("kafkaStream.tag") === col("rules.parameters")),"inner")
      .select(col("rules.deviceIdentifier").as("asset"),col("kafkaStream.tag").as("tag"),
        col("rules.ruleExpression").as("ruleExpression"),col("rules.outputTag").as("outputTag"),
        col("kafkaStream.timestamp").as("timestamp"),col("kafkaStream.value").as("value"),
        col("rules.uniqueParametersCount").as("uniqueParametersCount"),col("rules.ruleWaitTime").as("ruleWaitTime"),
        col("rules.ruleFrequency").as("ruleFrequency")
      ).as[JoinedRule](Encoders.product[JoinedRule])
  }

  def groupData(spark:SparkSession,joinedStream:Dataset[JoinedRule]):Dataset[State] ={
    import spark.implicits._
    val groupedStream = joinedStream.groupByKey(data => (data.outputTag,data.asset,data.timestamp,data.ruleExpression,data.uniqueParametersCount,data.ruleWaitTime,data.ruleFrequency)).flatMapGroupsWithState(OutputMode.Append(),GroupStateTimeout.NoTimeout())(updateAllStates)
    groupedStream.select(col("outputTag"),col("asset"),col("timestamp"),col("expression"),col("parameters"),col("expression"),col("uniqueParametersCount"),col("ruleWaitTime"),col("ruleFrequency")).as[State]
  }

  def computeRules(spark:SparkSession,groupedData:Dataset[State]) = {
    import spark.implicits._
    groupedData.map(data => {
      if(data.parameters.keySet.size == data.uniqueParametersCount){
        var sb = new StringBuilder(data.expression)
        for(repl <- data.parameters.keySet) yield {
          sb = new StringBuilder(sb.replaceAllLiterally(repl, data.parameters(repl).toString))
        }
        println(sb)
        val toolbox = currentMirror.mkToolBox()
        val tree = toolbox.parse(sb.toString())
        val output = toolbox.eval(tree) match {
          case false => 0
          case true => 1
          case _ => -999
        }
        Computed(data.outputTag,data.asset,data.timestamp,data.parameters,data.expression,data.uniqueParametersCount,data.ruleWaitTime,data.ruleFrequency,true,output)
      }
      else{
        Computed(data.outputTag,data.asset,data.timestamp,data.parameters,data.expression,data.uniqueParametersCount,data.ruleWaitTime,data.ruleFrequency,false,-999)
      }
    })
  }

  def updateState(state:State,input:JoinedRule):State ={
    val newState = State(state.outputTag,state.asset,state.timestamp,state.parameters.updated(input.tag,input.value),state.expression,state.uniqueParametersCount,state.ruleWaitTime,state.ruleFrequency)
    newState
  }
  def updateAllStates(key: (String,String,Long,String,Int,Int,Int),inputs:Iterator[JoinedRule],oldState:GroupState[State]):Iterator[State]={
    var state = if(oldState.exists) oldState.get else {
      State(key._1,key._2,key._3,Map().empty,key._4,key._5,key._6,key._7)
    }
    for(input <- inputs){
      state = updateState(state,input)
      oldState.update(state)
    }
    Iterator(state)
  }

}