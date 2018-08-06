package com.hongdd.spark.util

import net.sf.json.JSONObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaStreamUtil {
  def getDStream(ssc: StreamingContext):InputDStream[ConsumerRecord[String, String]] = {
    val properties = PropertiesUtil.getKafkaProperties(this)
    val bootstrap = properties.getProperty("bootstrap")
    val topic = properties.getProperty("topic")
    assert(bootstrap != null && topic != null, "kafka properties has null values")

    Logger.getRootLogger.setLevel(Level.WARN)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_test_kafka",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val dbIndex = 0
    val orderTotalKey = "app::user::click"

    val topics = Array(topic)
    val kafkaStream = KafkaUtils.createDirectStream(ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    kafkaStream
  }

  def getDStreamValue(ssc: StreamingContext): DStream[String] = {
    getDStream(ssc).map(_.value)
  }

  def defaultLogValue(ssc: StreamingContext): DStream[JSONObject] = {
    getDStreamValue(ssc).flatMap(line => Some(JSONObject.fromObject(line)))
  }
}
