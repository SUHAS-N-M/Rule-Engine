package org.github.ruleSource

import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext, SparkSession}
import org.github.ruleProcessor.RuleProcessor

object RuleSource {
  def fetchRules(): List[RawRule] ={
    List(
      RawRule("Asset1","A>B","A,B","OUTPUT_TAG1","5","2"),
      RawRule("Asset2","A>B","A,B","OUTPUT_TAG2","5","2"),
      RawRule("Asset1","A>C","A,C","OUTPUT_TAG3","-1","0")
    )
  }
  def getRules(spark:SparkSession)={
    import spark.implicits._
    val rules = fetchRules().toDS()
    rules.show()
    RuleProcessor.splitParameters(rules)
  }
}
