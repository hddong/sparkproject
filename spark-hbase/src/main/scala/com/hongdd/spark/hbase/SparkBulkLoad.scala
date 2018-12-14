package com.hongdd.spark.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes

object SparkBulkLoad {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setAppName("SearchHbaseDataWithKerberosByYarn").setMaster("local")
    val spark = SparkSession.builder().config(sconf).getOrCreate()

    val sc = spark.sparkContext
    val conf = HBaseConfiguration.create()

    val tableName = "test"
    val table = new HTable(conf, tableName)

    conf.set(TableInputFormat.INPUT_TABLE,tableName)
    val job = Job.getInstance()

    val conn = ConnectionFactory.createConnection(conf)
    val htable = conn.getTable(TableName.valueOf(tableName))

    job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass (classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)))

    val num = sc.parallelize(1 to 10)
    val rdd = num.map(x=>{
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "cf".getBytes(), "c1".getBytes(), "value_xxx".getBytes() )
      (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })

    job.getConfiguration.set("mapred.output.dir", "/tmp/iteblog")
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

    spark.stop()
  }
}
