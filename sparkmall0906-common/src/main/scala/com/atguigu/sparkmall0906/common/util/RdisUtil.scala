package com.atguigu.sparkmall0906.common.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
    private val config = ConfigurationUtil("config.properties")
    private val host = config.getString("redis.host")
    private val port = config.getInt("redis.port")
    
    private val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(false) //每次获得连接的进行测试
    private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, host, port)
    
    // 直接得到一个 Redis 的连接
    def getJedisClient: Jedis = {
        jedisPool.getResource
    }
    
    def main(args: Array[String]): Unit = {
        val client: Jedis = getJedisClient
        client.sadd("blacklist", "101")
    }
}
