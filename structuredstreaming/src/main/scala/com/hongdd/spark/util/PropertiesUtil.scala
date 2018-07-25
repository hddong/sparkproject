package com.hongdd.spark.util

import java.io.{File, FileInputStream}
import java.util.Properties


object PropertiesUtil {
  def getKafkaProperties[T](obj: T): Properties = {
    val properties = new Properties()
    val confPath = System.getProperty("user.dir") + File.separator + "conf/kafka.properties"
    val fileStream = if (new File(confPath).exists()) {
      new FileInputStream(confPath)
    } else {
      obj.getClass.getClassLoader.getResourceAsStream("kafka.properties")
    }
    properties.load(fileStream)
    properties
  }
}
