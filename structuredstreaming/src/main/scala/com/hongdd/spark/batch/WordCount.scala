package com.hongdd.spark.batch


import com.hongdd.spark.util.PropertiesUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object WordCount {

  def main(args: Array[String]): Unit = {
//    val properties = new Properties()
//    val fileStream = if (new File(confPath).exists()) {
//      new FileInputStream(confPath)
//    } else {
//      this.getClass.getClassLoader.getResourceAsStream("kafka.properties")
//    }
//    properties.load(fileStream)
    val properties = PropertiesUtil.getKafkaProperties(this)
    val bootstrap = properties.getProperty("bootstrap")
    val topic = properties.getProperty("topic")
    assert(bootstrap != null && topic != null, "kafka properties has null values")

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("batch-word-count")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    import spark.implicits._

    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .load()

    val words = lines.selectExpr("cast(value as string)")
      .as[String].flatMap(_.split(" "))
    val wordCount = words.groupBy("value").count()

    // no trigger. as soon as previous complete
    val query = wordCount.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()
    query.awaitTermination()
  }
}
