package com.hongdd.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WindowTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("window-test").getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    import spark.implicits._
    val df = List(
      ("user1", "2017-01-01", 50),
      ("user1", "2017-01-02", 45),
      ("user1", "2017-01-03", 55),
      ("user2", "2017-01-01", 43),
      ("user2", "2017-01-02", 58),
      ("user2", "2017-01-03", 49)
    ).toDF("user", "date", "count")

    import org.apache.spark.sql.expressions.Window
    val wSpac = Window.partitionBy("user").orderBy("date")

    println("-------test window between--------------")
    val wSpacWindow = wSpac.rowsBetween(-1, 1)
    import org.apache.spark.sql.functions._
    df.withColumn("movingAvg", avg('count).over(wSpacWindow)).show()

    println("-------test window between low--------------")
    val wSpacWindowLow = wSpac.rowsBetween(Long.MinValue, 1)
    df.withColumn("lowAvg", avg('count).over(wSpacWindowLow)).show()

    println("-------test window per one line--------------")
    // lag (row, num) 获取前面 第 num 行数据
    // lead (row, num) 获取后面 第 num 行数据
    df.withColumn("lowAvg", lag('count, 1).over(wSpac)).show()

    println("-------test window 分组排名--------------")
    df.withColumn("lowAvg", rank().over(wSpac)).show()

    spark.stop()
  }
}
