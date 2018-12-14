package com.hongdd.spark.kerberos.hbase

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SearchHbaseDataWithKerberos {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SearchHbaseDataWithKerberosByYarn")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    //create SparkContext

    //create hbase config
    val config = HBaseConfiguration.create()
    val tableName = "test"
    config.set("hadoop.security.authentication", "kerberos")
    config.set("hbase.security.authentication", "kerberos")
    config.set("hbase.master.kerberos.principal", "hbase/_HOST@KDC300")
    config.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@KDC300")
    //    config.set("hbase.zookeeper.quorum","docker1.cmss.com,docker2.cmss.com,docker3.cmss.com")
    //    config.set("zookeeper.znode.parent", "/hbase-unsecure")
    config.set("hbase.zookeeper.property.clientPort","2181")


    config.set(TableInputFormat.INPUT_TABLE,tableName)

    // login from keytab
    val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(conf.get("spark.yarn.principal"), conf.get("spark.yarn.keytab"));
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
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
      }
    })
    spark.stop()
  }
}
