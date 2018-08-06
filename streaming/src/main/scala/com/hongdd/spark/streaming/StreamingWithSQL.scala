package com.hongdd.spark.streaming

import com.hongdd.spark.util.KafkaStreamUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * spark Streaming 转 spark sql 处理数据
  * Structured Streaming完美解决
  */
object StreamingWithSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("streaming-word-count")
    val spark = SparkSession.builder().config(conf)
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaValues = KafkaStreamUtil.defaultLogValue(ssc)

    kafkaValues.foreachRDD { jsonRDD =>
      val sqlContext = spark.getOrCreate()
      import sqlContext.implicits._
      val logDataFrame = sqlContext.createDataFrame(
        jsonRDD.map(t => (t.getString("uid"), t.getString("click_count"))))
          .toDF("uid", "click")
      logDataFrame.groupBy($"uid").count().show()
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
