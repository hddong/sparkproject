package com.hongdd.spark.batch

import java.io.Serializable

import com.hongdd.spark.util.{PropertiesUtil, RedisForeachWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType

object RedisSink {
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
    val schema = StructType.fromDDL("uid string, event_time string, os_type string, click_count int")
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .load()
    val datas = lines.selectExpr("cast(value as string) as json")
      .select(from_json($"json", schema = schema).as("data"))
      .select("data.uid", "data.click_count")
      .groupBy("uid").count()

    val query = datas.writeStream
        .outputMode("update")
        .foreach(new RedisForeachWriter)
        .start()

    query.awaitTermination()
  }

}
