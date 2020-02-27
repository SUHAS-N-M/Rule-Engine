package org.github.kafkaSource

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._

object KafkaSource {
  def readKafkaStream(spark:SparkSession) = {
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","diag_test_input")
      .load()
    //kafkaStream.show()
    import spark.implicits._
    val formattedValueStream = formatValueFromStream(spark,kafkaStream)
    formattedValueStream
  }
  def formatValueFromStream(spark:SparkSession,source:DataFrame) ={
    import spark.implicits._
    source.selectExpr("cast(value as String)").withColumn("temp",split(col("value"),"\\$"))
      .select(col("temp").getItem(0).cast("Long").as("timestamp")
        ,col("temp").getItem(1).as("asset")
        ,col("temp").getItem(2).as("tag")
        ,col("temp").getItem(3).cast("Double").as("value")
      ).as[Data]
  }
}
