import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RddUseSql {
  @transient val conf = new SparkConf().setMaster("local").setAppName("test")
  @transient val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    val t1 = sc.parallelize(Array(1, 2,3 ,4))
    val sql = SQLContextSingletonT.getInstance(sc)
//    val sql = new SQLContext(sc)
    val schemaString = "name age"
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, IntegerType, true)))
    val t = sc.parallelize(Array((1, 2), (3, 4))).map(p => Row(p._1, p._2))
    sql.createDataFrame(t, schema).registerTempTable("test")
    sql.sql("select * from test").show()

    t1.foreach{f =>
      val sql = SQLContextSingletonT.getInstance(sc)
      sql.sql("select * from test").show()
    }

  }
}
