package com.atguigu.sparkmall0906.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall0906.common.util.RedisUtil
import com.atguigu.sparkmall0906.realtime.bean.AdsInfo
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsApp {
    def statLastHourAds(filteredDStream: DStream[AdsInfo]) = {
        val dateFormatter = new SimpleDateFormat("HH:mm")
        // 1. 利用窗口来对DStream进行开窗
        val DStreamWithWindow: DStream[AdsInfo] = filteredDStream.window(Minutes(60), Seconds(4))
        val hourMinutesCount = DStreamWithWindow.map(adsInfo => {
            ((adsInfo.adsId, dateFormatter.format(new Date(adsInfo.ts))), 1)
        }).reduceByKey(_ + _).map {
            case ((adsId, hourMinutes), count) => (adsId, (hourMinutes, count))
        }
        // 2. 转成json格式的字符串
        val adsIdHourMintesJson: DStream[(String, String)] = hourMinutesCount.groupByKey.map {
            case (adsId, hourMinutsCountIt) => {
                import org.json4s.JsonDSL._
                (adsId, JsonMethods.compact(JsonMethods.render(hourMinutsCountIt)))
            }
        }
        // 3. 写入redis
        adsIdHourMintesJson.foreachRDD(rdd => {
            val client: Jedis = RedisUtil.getJedisClient
            val result: Array[(String, String)] = rdd.collect
            result.foreach(println)
            import scala.collection.JavaConversions._
            client.hmset("last:hour:ads:click", result.toMap)
            client.close()
        })
    }
}

/*
=> RDD[]

// RDD[(广告1, 08:00), 1]
=> RDD[(adsId, hourMinutes), 1]  reduceByKey
=> RDD[(adsId, hourMinutes), count]     map
=> RDD[adsId, (hourMinutes, count)]    groupBy
=> RDD[adsId, Iterable[(hourMinutes, count)]]  map  把迭代器变成json格式的字符串
=> Map[adsId, hourMinutesCountJsonString]
client.hmset("last:hour:ads:click", Map[adsId, hourMinutesCountJsonString])



  /*result.foreach{
                case (adsId, json) => {
                    client.hset()
                }
            }*/
 */