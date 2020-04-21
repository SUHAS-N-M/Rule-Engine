package org.github

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.github.kafkaSource.KafkaSource
import org.github.ruleProcessor.RuleProcessor
import org.github.ruleSource.RuleSource
import org.github.kafkaSink.KafkaSink
import scala.concurrent.duration._

object App  {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Softwares\\HadoopWinUtils")
    println( "Hello World!" )
    val spark = SparkSession
      .builder()
      .appName("DiagnosticEngine")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    val rules = RuleSource.getRules(spark).as("rules")
    //RuleProcessor.splitParameters(rules)
    val kafkaStream = KafkaSource.readKafkaStream(spark).as("kafkaStream")
    val joinedStream = RuleProcessor.joinWithRules(kafkaStream,rules)
    val groupedData = RuleProcessor.groupData(spark,joinedStream)
    val computedRuleData = RuleProcessor.computeRules(spark,groupedData)
    val filteredOutput = KafkaSink.getFilteredRecords(spark,computedRuleData)
    filteredOutput.writeStream.format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Append())
      //.trigger(Trigger.ProcessingTime(10.seconds))
      .option("checkpointLocation", "E:/PersonalProjects/RuleEngine/Checkpoint")
      .start()
      .awaitTermination()
  }
}
