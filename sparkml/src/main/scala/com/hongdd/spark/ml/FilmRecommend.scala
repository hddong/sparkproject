package com.hongdd.spark.ml

import org.apache.spark.sql.SparkSession

/**
  * com.hongdd.spark.mllib.FileRecommend
  */
object FilmRecommend {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ml-film-recommend")
      .master("local[2]")
      .getOrCreate()
  }
}
