package com.hongdd.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

object SparkBulkload2 {
  val zookeeperQuorum = "172.16.13.185:2181"
  val dataSourcePath = "file:///D:/data/news_profile_data.txt"
  val hdfsRootPath = "hdfs://test/"
  val hFilePath = "hdfs://test/hbase/bulkload/hfile/"
  val tableName = "news"
  val familyName = "cf1"
  val qualifierName = "title"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(s"${this.getClass.getSimpleName}").master("local").getOrCreate()
//    val sc = new SparkContext(sparkConf)
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", hdfsRootPath)
    val fileSystem = FileSystem.get(hadoopConf)
    val hbaseConf = HBaseConfiguration.create(hadoopConf)
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin

    // 0. 准备程序运行的环境
    // 如果 HBase 表不存在，就创建一个新表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      val hcd = new HColumnDescriptor(familyName)
      desc.addFamily(hcd)
      admin.createTable(desc)
    }
    // 如果存放 HFile文件的路径已经存在，就删除掉
    if(fileSystem.exists(new Path(hFilePath))) {
      fileSystem.delete(new Path(hFilePath), true)
    }

    // 1. 清洗需要存放到 HFile 中的数据，rowKey 一定要排序，否则会报错：
    // java.io.IOException: Added a key not lexically larger than previous.

    val data = spark.sparkContext.textFile(dataSourcePath)
      .map(jsonStr => {
        // 处理数据的逻辑
        val rowkey = "id"
        val title = "title"
        (rowkey, title)
      })
      .sortByKey()
      .map(tuple => {
        val kv = new KeyValue(Bytes.toBytes(tuple._1), Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), Bytes.toBytes(tuple._2))
        (new ImmutableBytesWritable(Bytes.toBytes(tuple._1)), kv)
      })

    // 2. Save Hfiles on HDFS
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, new HTableDescriptor(TableName.valueOf(tableName)))

    data.saveAsNewAPIHadoopFile(
      hFilePath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    //  3. Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    val regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName))
    bulkLoader.doBulkLoad(new Path(hFilePath), admin, table, regionLocator)

    hbaseConn.close()
    fileSystem.close()
    spark.stop()
  }
}
