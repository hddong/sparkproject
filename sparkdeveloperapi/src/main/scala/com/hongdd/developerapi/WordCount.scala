package com.hongdd.developerapi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FileMerge-1")
      .master("local")
      .getOrCreate()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    spark.createDataFrame(Array((1, 1), (2, 2))).toDF("id", "data").show()

    spark.stop()
  }
}
