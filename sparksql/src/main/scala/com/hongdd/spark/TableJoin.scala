package com.hongdd.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TableJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sql-join-test")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val start = System.currentTimeMillis();
    spark.sql("use sparktest")
    val movies = spark.table("movies")
    val ratings = spark.table("ratings")

    ratings.join(movies, "movieid").show(true)

    val end = System.currentTimeMillis()
    print("use time: " + (end-start))
    spark.stop()
  }
}
