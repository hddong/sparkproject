package com.hongdd.spark.util

import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.Jedis

class RedisForeachWriter extends ForeachWriter[Row] {

  var jedis:Jedis = _
  val dbIndex = 0
  val orderTotalKey = "app::user::click"
  override def open(partitionId: Long, version: Long): Boolean = {
    jedis = RedisClient.pool.getResource
    jedis.select(dbIndex)
    true
  }

  override def process(row: Row): Unit = {
    jedis.hincrBy(orderTotalKey, row.getString(0), row.getLong(1))
  }

  override def close(errorOrNull: Throwable): Unit = {
    jedis.close()
  }
}
