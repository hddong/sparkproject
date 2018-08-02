package com.hongdd.spark.streaming

import com.hongdd.spark.util.PropertiesUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WorkCount {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtil.getKafkaProperties(this)
    val bootstrap = properties.getProperty("bootstrap")
    val topic = properties.getProperty("topic")
    assert(bootstrap != null && topic != null, "kafka properties has null values")

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("streaming-word-count")
    val ssc = new StreamingContext(conf, Seconds(2))

    Logger.getRootLogger.setLevel(Level.WARN)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_test_kafka",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    val kafkaStream = KafkaUtils.createDirectStream(ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val newStream = kafkaStream.flatMap(_.value().split(" "))
    val wordCount = newStream.map((_, 1)).reduceByKey(_ + _)
    wordCount.foreachRDD{data =>
      data.foreachPartition{ line =>
      }
    }
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
