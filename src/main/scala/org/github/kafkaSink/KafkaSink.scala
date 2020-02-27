package org.github.kafkaSink

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.github.ruleProcessor.Computed

import scala.collection.immutable.HashMap

object KafkaSink {
  def write(output:Dataset[RuleState])={
    output.writeStream
      .format("kafka")
      .option("broker-list","localhost:9092")
      .option("topic","diag_test_output")
  }
  def getFilteredRecords(spark:SparkSession,computedRules:Dataset[Computed]): Dataset[RuleState] ={
    import spark.implicits._
    val updatedWindowRecords = computedRules.where(computedRules.col("ruleWaitTime").notEqual(lit(-1)) )
      .groupByKey(data => (data.asset,data.outputTag)).flatMapGroupsWithState(OutputMode.Append(),GroupStateTimeout.NoTimeout())(updateAllWindowRules).as[RuleState]
    val updatedPointInTimeRecords = computedRules.where(computedRules.col("ruleWaitTime").equalTo(lit(-1))).groupByKey(data => (data.asset,data.outputTag)).flatMapGroupsWithState(OutputMode.Append(),GroupStateTimeout.NoTimeout())(updateAcrossRules).as[RuleState]
    val updatedRecords = updatedWindowRecords.union(updatedPointInTimeRecords).where(col("publishFlag") === lit(true))
    updatedRecords
  }
  def updateWindowRules(input:Computed,state:WindowRuleState): WindowRuleState ={
    val ruleFrequency = input.ruleFrequency
    val ruleWaitTime = input.ruleWaitTime
    val currentTimestamp = input.timestamp
    val newValueMap:HashMap[Long,Double] = state.values.filter(t=>{
      if(t._1 >= currentTimestamp - ruleWaitTime) true else false
    }).updated(input.timestamp,input.computedValue).filter(t => t._2 == 1.0)
    println(newValueMap)
    val computedValue = if(newValueMap.keySet.size > ruleFrequency) 1 else 0
    WindowRuleState(state.asset,state.outputTag,currentTimestamp,newValueMap,computedValue)
  }
  def updateAllWindowRules(key: (String,String),inputs:Iterator[Computed],oldState:GroupState[WindowRuleState]):Iterator[RuleState] = {
    var state = if(oldState.exists) oldState.get else {
      WindowRuleState(key._1,key._2,0,HashMap.empty,-999)
    }
    val previousState = state.computedValue
    for (input <- inputs){
      state = updateWindowRules(input,state)
      oldState.update(state)
    }
    val currentState = state.computedValue
    val publishFlag = if(previousState != currentState && currentState != -999) true else false
    Iterator(RuleState(key._1,key._2,state.computedValue,state.timestamp,publishFlag))
  }
  def updateRule(input:Computed,state:RuleState): RuleState ={
    var newState:RuleState = null
    if(state.currentState != input.computedValue && input.computedValue != -999){
      newState = RuleState(state.asset,state.outputTag,input.computedValue,input.timestamp,true)
    }
    else{
      newState = RuleState(state.asset,state.outputTag,state.currentState,input.timestamp,false)
    }
    newState
  }
  def updateAcrossRules(key: (String,String),inputs:Iterator[Computed],oldState:GroupState[RuleState]):Iterator[RuleState] ={
    var state = if (oldState.exists) oldState.get else {
      RuleState(key._1,key._2,-999,0,false)
    }
    for (input <- inputs){
      state = updateRule(input,state)
      oldState.update(state)
    }
    Iterator(state)
  }
}