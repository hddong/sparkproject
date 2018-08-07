package com.hongdd.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object ALSFilmRecommend {
  def main(args: Array[String]): Unit = {
    // 日志设置
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("als-recommendation-test")
      .master("local")
      .getOrCreate()
    ml(spark)

    spark.stop()
  }
  // 定义评分类
  case class Rating(userId:Int, movieId:Int, rating:Float, time:Long)
  // ml包调用方式
  def ml(spark:SparkSession):Unit = {

    // 评分转换方法
    def parseRating(str:String):Rating ={
      val fields = str.split(s"\t")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }
    //
    // spark2对dataset的操作, 需要进行相应的encode操作
    // 否则报错: Unable to find encoder for type stored in a Dataset
    // 这里引用全部隐式转换 spark.implicits._
    // spark.implicits._支持 case class
    // tips:
    //   case class 必须定义在函数主体外面
    // 也可以对 对应结构 转换

    import spark.implicits._
    val ratings = spark.read.textFile("./src/main/resources/MLlibData/recommend/u.data")
      .map(parseRating)
      .toDF()
    ratings.show()
    // 随机划分数据
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // 使用ALS算法和训练数据构造推荐模型
    // 使用ml包中的ALS算法
    // params:
    //   setRank设置随机因子
    //   setMaxiter设置最大迭代次数
    //   setRegParam设置正则化参数，类比lambda
    //   setUserCol设置用户id列名
    //   setItemCol设置物品列名
    //   setRatingCol设置打分列名
    val als = new ALS()
      .setRank(10)
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    // 使用训练数据生成模型
    val model = als.fit(training)
    // 使用测试数据进行预测
    val prediction = model.transform(test)
    println("预测结果:")
    prediction.show()
    // 评估预测结果
    // RegressionEvaluator这个类是用户评估预测效果的，预测值与原始值
    // setLabelCol要和als设置的setRatingCol一致
    // RegressionEvaluator的setPredictionCol必须是prediction因为，ALSModel的默认predictionCol也是prediction
    // 如果要修改的话必须把ALSModel和RegressionEvaluator一起修改
    // model.setPredictionCol("prediction")和evaluator.setPredictionCol("prediction")
    // setMetricName这个方法，评估方法的名字:
    //   rmse-平均误差平方和开根号
    //   mse-平均误差平方和
    //   mae-平均距离（绝对）
    //   r2-没用过不知道
    // 这里建议就是用rmse就好了，其他的基本都没用，当然还是要看应用场景，这里是预测分值就是用rmse。如果是预测距离什么的mae就不从，看场景
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    // 评估预测
    // tips:
    //   数据集过大会导致NaN
    val rmse = evaluator.evaluate(prediction)
    println("root-mean-square error: " + rmse)
  }
}
