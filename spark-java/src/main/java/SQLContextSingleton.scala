import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * stream 转 sql使用类
  * 确保在stream中使用同一个SQLContext
  */
object SQLContextSingletonT {
  @transient private var instance: SQLContext = _
  @deprecated("Use SparkSession.builder instead", "2.0.0")
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance.synchronized{
      instance
    }
  }
}
