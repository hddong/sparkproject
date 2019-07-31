package com.hongdd.kafka.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

class DefaultProducer {
  def createProducer(brokers: String, topic: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG /* 同"key.serializer"必须配置*/ ,
      classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG /* 同"value.serializer"必须配置*/ ,
      classOf[StringSerializer].getName)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG /* 同"bootstrap.servers" 必须配置*/ , brokers)
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")

    new KafkaProducer[String, String](props)
  }
}
