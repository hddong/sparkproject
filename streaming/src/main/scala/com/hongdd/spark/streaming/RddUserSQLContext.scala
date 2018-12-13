package com.hongdd.spark.streaming

import org.apache.spark.sql.SparkSession

object RddUserSQLContext {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val sc = spark.sparkContext
    val t1 = sc.parallelize(Array(1, 2, 3, 4))
    spark.createDataFrame(Array(1, 2, 3, 4, 5).map(Tuple1.apply)).toDF("id").createTempView("test")

    spark.sql("select * from test").show()

    t1.foreach{ f =>
      val sp = SparkSession.builder().getOrCreate()
      sp.sql("select * from test").show()
    }
    spark.stop()
  }
}
