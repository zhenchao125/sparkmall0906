package com.atguigu.sparkmall0906.realtime.app

import com.atguigu.sparkmall0906.common.util.RedisUtil
import com.atguigu.sparkmall0906.realtime.bean.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DayAreaCityAdsApp {
    def statAreaCityAdsPerDay(adsInfoDStream: DStream[AdsInfo], sc: SparkContext) = {
        sc.setCheckpointDir("hdfs://hadoop201:9000/sparkmall0906")
        val key = "day:area:city:ads"
        // 1. 统计数据
        val resultDSteam: DStream[(String, Int)] = adsInfoDStream.map(info => (info.toString, 1)).reduceByKey(_ + _)
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
                Some(seq.sum + opt.getOrElse(0))
            })
        
        
        // 2. 写入到redis
        resultDSteam.foreachRDD(rdd => {
            val client: Jedis = RedisUtil.getJedisClient
            val totalCountArray: Array[(String, Int)] = rdd.collect
            totalCountArray.foreach{
                case (field, count) =>{
                    client.hset(key, field, count.toString)
                }
            }
            client.close()
        })
    }
}

/*

1.统计数据
    先转换类型:   (day:area:city:ads, 1) =>
                 (day:area:city:ads, long) =>
                 updateStateByKey(?)


2. 写入到redis

 */