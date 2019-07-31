package com.hongdd.kafka

import java.util.{Collections, Properties}

import com.hongdd.kafka.Consumer.{BOOTSTRAP, GROUP_ID}
import kafka.tools.SimpleConsumerShell
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

class Consumer {
  def kafkaConfig(bootstrap: String,
                  groupId: String,
                  reset: String = "earliest"): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG /**/, bootstrap)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG /**/, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    //    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString) /* 随机groupId从头获取数据 */
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true.toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
      reset /* latest表示接受接收最大的offset(即最新消息),earliest表示最小offset,即从topic的开始位置消费所有消息. */)
    props
  }
}

object Consumer {
  def ZK_CONN = "10.139.19.81:2181"
  def BOOTSTRAP = "10.139.19.81:9092"
  def GROUP_ID = "test_scala_consumer_group"
  def TOPIC = "test"

  def main(args: Array[String]): Unit = {
    println("开始读取kafka数据")

    val consumer = new KafkaConsumer[String, String](new Consumer().kafkaConfig(BOOTSTRAP, GROUP_ID))
    consumer.subscribe(Collections.singleton(TOPIC))

    consumer.seekToBeginning(consumer.assignment())
    val records = consumer.poll(10000)
    //    println(records.count())
    for (record <- records.asScala) {
      println(record)
    }
    consumer.close()
    SimpleConsumerShell
  }


}
