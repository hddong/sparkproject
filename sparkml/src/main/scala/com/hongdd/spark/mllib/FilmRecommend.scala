package com.hongdd.spark.mllib

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

/**
  * com.hongdd.spark.ml.FileRecommend
  */
object FilmRecommend {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("mllib-film-recommend")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use sparktest")
    val ratingData = spark.table("ratings").rdd.map(f =>
      (f.getString(0), f.getString(1), f.getInt(2), f.getString(3)))
    val ratings = ratingData.map(rating =>
    rating match {
      case (user, item, rate, time) => Rating(user.toInt, item.toInt, rate.toDouble)
    }).cache()

    val users = ratings.map(_.user).distinct().count()
    val products = ratings.map(_.product).distinct().count()
    val rates = ratings.count()
    println(s"count: $users, from $products user with $rates products")

    val splits = ratings.randomSplit(Array(0.8, 0.2), seed = 111)
    val (training, testing) = (splits(0), splits(1))

    //模型
    val rank = 12 //
    val lambad = 0.01 //正则化
    val iter = 5
    // 训练模型
    val model = ALS.train(training, rank, iter, lambad)

    val result = model.recommendProducts(1, 5)
    print(result)
    //预测和分析
//    val userProducts = testing.map{case Rating(user, product, rate) => (user, product)}
//    val predict = model.predict(userProducts)
//
//    val testData = spark.createDataFrame(testing)
//    val predictData = spark.createDataFrame(predict)
//
//    testData.join(predictData, Seq("user", "product"))

  }
}
