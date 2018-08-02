package com.hongdd.spark.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Redis 操作类
  */
object RedisClient extends Serializable {
  val redisHost = "192.168.56.172"
  val redisPort = 6379
  val redisTimeout = 30000

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run(): Unit = {
      print("execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run())
}
