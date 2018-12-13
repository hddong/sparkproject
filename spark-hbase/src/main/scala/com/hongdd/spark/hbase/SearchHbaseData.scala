package com.hongdd.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SearchHbaseData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SearchHbaseDataWithKerberosByYarn").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //create hbase config
    val config = HBaseConfiguration.create()
    val tableName = "test"

    config.set("hbase.zookeeper.quorum",
      Option(conf.get("hbase.zookeeper.quorum")).getOrElse("docker1.cmss.com,docker2.cmss.com,docker3.cmss.com"))
//    config.set("hbase.zookeeper.quorum","docker1.cmss.com,docker2.cmss.com,docker3.cmss.com")
    config.set("zookeeper.znode.parent", "/hbase-unsecure")
    config.set("hbase.zookeeper.property.clientPort","2181")

    config.set(TableInputFormat.INPUT_TABLE,tableName)

    val hbaseRDD = sc.newAPIHadoopRDD(config,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count = hbaseRDD.count()
    println("total count:" + count)
    hbaseRDD.map(x => x._2).map(_.listCells).foreach{x =>
      val keyValue = x.get(0)
      val rowKey = Bytes.toString(keyValue.getRowArray)
      val family = Bytes.toString(keyValue.getFamilyArray)
      val qualifier = Bytes.toString(keyValue.getQualifierArray)
      val timestamp = keyValue.getTimestamp
      val value = Bytes.toString(keyValue.getValueArray)
      println((rowKey,family,qualifier,timestamp,value))
    }
    spark.stop()
  }
}