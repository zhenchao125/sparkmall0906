package com.atguigu.sparkmall0906.realtime.app

import java.util

import com.atguigu.sparkmall0906.common.util.RedisUtil
import com.atguigu.sparkmall0906.realtime.bean.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListApp {
    val countKey = "day:userid:adsid"
    val blackList = "blackList"
    
    /**
      * 把在黑名单中的用户的点击的广告记录给去除
      *
      * @param adsInfoDStream
      */
    def filterBlackList(adsInfoDStream: DStream[AdsInfo], sc: SparkContext) = {
        adsInfoDStream.transform {
            rdd => {
                val client: Jedis = RedisUtil.getJedisClient
                // 1. 先拿到黑名单
                val blackUids: util.Set[String] = client.smembers(blackList)
                client.close()
                // 把黑名单使用广播变量.
                val blackListBC: Broadcast[util.Set[String]] = sc.broadcast(blackUids)
                // 2. 过滤
                rdd.filter {
                    info => {
                        !blackListBC.value.contains(info.userId)
                    }
                }
            }
        }
    }
    
    /**
      * 检测用户是否添加到黑名单中
      * @param adsInfoDStream
      */
    def checkUserToBlackList(adsInfoDStream: DStream[AdsInfo]) = {
        adsInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition(infoIt => {
                val jedisClient: Jedis = RedisUtil.getJedisClient
                infoIt.foreach(info => {
                    // 1. 每个用户每天对每个广告的点击次数写入了redis
                    val field = s"${info.dayString}:${info.userId}:${info.adsId}"
                    jedisClient.hincrBy(countKey, field, 1L)
                    
                    // 2. 判断次数是否超过了阈值, 超过则写入到黑名单中
                    if (jedisClient.hget(countKey, field).toLong >= 100000) {
                        jedisClient.sadd(blackList, info.userId)
                    }
                })
                jedisClient.close()
            })
        })
    }
    
    
}

/*
val jedisClient: Jedis = RedisUtil.getJedisClient
al key = "day:userid:adsid"
                        val field = s"${info.dayString}:${info.userId}:${info.adsId}"
                        jedisClient.hincrBy(key, field, 1L)
添加用户到黑名单:
    当某个用户某天对某一个广告的点击此处超过了100, 把他加入到黑名单
    
    交给 redis 来进行技术
      
      key:   "day:userid:adsid"
        value:   field                  value
                 2019-03-23:101:1        2
                 2019-03-23:102:1        3

    
 */
