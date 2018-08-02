package com.hongdd.spark.streaming

import com.hongdd.spark.util.{PropertiesUtil, RedisClient}
import net.sf.json.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object RedisSink {
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

    val dbIndex = 0
    val orderTotalKey = "app::user::click"

    val topics = Array(topic)
    val kafkaStream = KafkaUtils.createDirectStream(ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val events = kafkaStream.flatMap(line =>
      Some(JSONObject.fromObject(line.value()))
    )
    val userClick = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)
    userClick.foreachRDD{eventRDDs =>
      eventRDDs.foreachPartition{ eventRDD =>
        // 连接redis
        val jedis = RedisClient.pool.getResource
        jedis.select(dbIndex)
        eventRDD.foreach { event =>
          jedis.hincrBy(orderTotalKey, event._1, event._2)
        }
        // 关闭连接
        if (jedis != null) jedis.close()
      }
    }
    userClick.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
