package com.hongdd.spark.mllib

import breeze.linalg.rank
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.collection.mutable

/**
  * 官网示例
  */
object ALSFilmRecommend {
  // 设置参数类
  // 参数含义
  // input表示数据路径
  // kryo表示是否使用kryo序列化
  // numIterations迭代次数
  // lambda正则化参数
  // numUserBlocks用户的分块数
  // numProductBlocks物品的分块数
  // implicitPrefs这个参数没用过，但是通过后面的可以推断出来了，是否开启隐藏的分值参数阈值，预测在那个级别才建议推荐，这里是5分制度的，详细看后面代码
  // AbstractParams在spark-examples中定义, 这里直接拷贝了源代码
  case class Params(input:String = null,
                    output:String = null,
                    kryo:Boolean = false,
                    numIterations:Int = 20,
                    lambda:Double = 1.0,
                    rank:Int = 10,
                    numUserBlocks:Int = -1,
                    numProductBlocks:Int = -1,
                    implicitPrefs:Boolean = false) extends AbstractParams[Params]
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("MovieLensALS"){
      head("MovieLensALS: an example app for ALS on MovieLens data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Unit]("kryo")
        .text("use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks} (auto)")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numProductBlocks")
        .text(s"number of product blocks, default: ${defaultParams.numProductBlocks} (auto)")
        .action((x, c) => c.copy(numProductBlocks = x))
      opt[Unit]("implicitPrefs")
        .text("use implicit preference")
        .action((_, c) => c.copy(implicitPrefs = true))
      // 在执行过程中，input是required，所以需要添加它的program arguments:
      //    "/Users/sunyonggang/Downloads/spark-1.5.2/data/mllib/sample_movielens_data.txt"
      arg[String]("<input>")
        .required()
        .text("input paths to a MovieLens dataset of ratings")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.MovieLensALS \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --rank 5 --numIterations 20 --lambda 1.0 --kryo \
          |  data/mllib/sample_movielens_data.txt
        """.stripMargin)
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params:Params): Unit = {
    // 日志设置
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // .set("spark.sql.warehouse.dir","E:/ideaWorkspace/ScalaSparkMl/spark-warehouse")
    val conf = new SparkConf()
    // 序列化方式判断
    if (params.kryo) {
      conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating]))
        .set("spark.kryoserializer.buffer", "8m")
    }
    val spark = SparkSession.builder()
      .appName("mllib-als-recommendation-test")
      .master("local")
      .config(conf)
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    // 推荐的阈值 true有设定阈值 false不设定
    import spark.implicits._
    val implicitPrefs = params.implicitPrefs
    val ratings = spark.read.textFile(params.input)
      .map{ line =>
        val fields = line.split(s"\t")
        if (implicitPrefs) {
          Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble-2.5)
        } else {
          Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
        }
      }.cache()

    //计算一共有多少样本数
    val numRatings = ratings.count()
    //计算一共有多少用户
    val numUsers = ratings.map(_.user).distinct().count()
    //计算应该有多少物品
    val numMovies = ratings.map(_.product).distinct().count()

    //
    // DataSet另一种写法
    // val numUsers = ratings.select($"user").distinct().count()
    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    // 切分训练和预测数据
    val Array(training, testing) = ratings.randomSplit(Array(0.8, 0.2))
    training.cache()

    //构建测试样
    //分值为0表示，我对物品的评分不知道，一个积极有意义的评分表示：有信心预测值为1
    //一个消极的评分表示：有信心预测值为0
    //在这个案列中，我们使用的加权的RMSE，这个权重为自信的绝对值（命中就为1，否则为0）
    //关于误差，在预测和1,0之间是不一样的，取决于r 是正，还是负
    //这里splits已经减了分值阈值了，所以>0 =1 else 0的含义是，1表示分值是大于分值阈值的，这里是大于2.5,0表示小于2.5
    val test = if (params.implicitPrefs) {
      /*
       * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
       * Negative values means "confident that the prediction should be 0".
       * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
       * the confidence. The error is the difference between prediction and either 1 or 0,
       * depending on whether r is positive or negative.
       */
      testing.map(x => Rating(x.user, x.product, if (x.rating > 0) 1.0 else 0.0))
    } else {
      testing
    }.cache()

    ratings.unpersist(blocking = false)

    //setRank设置随机因子，就是隐藏的属性
    //setIterations设置最大迭代次数
    //setLambda设置正则化参数
    //setImplicitPrefs 是否开启分值阈值
    //setUserBlocks设置用户的块数量，并行化计算,当特别大的时候需要设置
    //setProductBlocks设置物品的块数量
    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(training.rdd)

    //训练的样本和测试的样本的分值全部是减了2.5分的
    //测试样本的分值如果大于0为1，else 0，表示分值大于2.5才预测为Ok

    //计算rmse
    val rmse = computeRmse(model, testing.rdd, params.implicitPrefs)

    println(s"Test RMSE = $rmse.")

    //保存模型，模型保存路劲为
    model.save(spark.sparkContext,params.output)
    println("模型保存成功，保存路劲为："+params.output)

    spark.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean)
  : Double = {

    //内部方法含义如下
    // 如果已经开启了implicitPref那么，预测的分值大于0的为1，小于0的为0，没有开启的话，就是用原始分值
    //min(r,1.0)求预测分值和1.0那个小，求小值，然后max(x,0.0)求大值， 意思就是把预测分值大于0的为1，小于0 的为0
    //这样构建之后预测的预测值和测试样本的样本分值才一直，才能进行加权rmse计算
    def mapPredictedRating(r: Double): Double = {
      if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
    }

    //根据模型预测，用户对物品的分值，predict的参数为RDD[(Int, Int)]
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))

    //mapPredictedRating把预测的分值映射为1或者0
    //join连接原始的分数,连接的key为x.user, x.product
    //values方法表示只保留预测值，真实值
    val joinData = data.map(x => ((x.user, x.product), x.rating))
    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(joinData).values

    //最后计算预测与真实值的平均误差平方和
    //这是先每个的平方求出来，然后再求平均值，最后开方
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
}
