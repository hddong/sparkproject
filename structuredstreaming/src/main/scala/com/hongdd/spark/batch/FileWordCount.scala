package com.hongdd.spark.batch

import com.hongdd.spark.util.PropertiesUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, TimestampType}

object FileWordCount {
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

    val userSchema = StructType.fromDDL("name string, age int")
    val lines = spark.readStream.option("sep", ",")
      .schema(userSchema)
      .csv("d:/tmp2")

    val query = lines.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
