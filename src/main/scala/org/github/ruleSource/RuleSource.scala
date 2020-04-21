package org.github.ruleSource

import java.util.Properties

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row, SQLContext, SparkSession}
import org.github.ruleProcessor.RuleProcessor

object RuleSource {
  def fetchRules(spark:SparkSession): Dataset[RawRule] ={
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user","postgres")
    connectionProperties.setProperty("password","postgres")
    import spark.implicits._
    spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres","rule_source",connectionProperties)
      .withColumn("ruleWaitTime",col("rule_wait_time")).withColumn("ruleFrequency",col("rule_frequency"))
      .withColumn("outputTag",col("output_tag")).as[RawRule]
  }
  def getRules(spark:SparkSession)={
    val rules = fetchRules(spark)
    //rules.show()
    RuleProcessor.splitParameters(rules)
  }
}
