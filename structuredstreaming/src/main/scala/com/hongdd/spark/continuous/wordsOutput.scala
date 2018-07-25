package com.hongdd.spark.continuous

import com.hongdd.spark.util.PropertiesUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object wordsOutput {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtil.getKafkaProperties(this)
    val bootstrap = properties.getProperty("bootstrap")
    val topic = properties.getProperty("topic")
    assert(bootstrap != null && topic != null, "kafka properties has null values")

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("batch-word-count")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    import spark.implicits._

    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .load()

    val query = lines.selectExpr("cast(key as string)", "cast(value as string)", "cast(offset as long)")
      .as[(String, String, Long)]
      .writeStream
      .format("console")
      .trigger(Trigger.Continuous("1 seconds"))
      .start()

    query.awaitTermination()
  }
}
