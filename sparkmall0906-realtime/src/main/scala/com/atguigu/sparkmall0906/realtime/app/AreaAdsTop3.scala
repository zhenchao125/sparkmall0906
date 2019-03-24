package com.atguigu.sparkmall0906.realtime.app

import java.util

import com.atguigu.sparkmall0906.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object AreaAdsTop3 {
    def statAreaAdsTop3(dayAreaAdsCityCount: DStream[(String, Int)]) ={
        // 1. 去掉城市   并且统计了地区的广告点击量
        val dayAreaAdsCountDStream= dayAreaAdsCityCount.map {
            case (dayAreaAdsCity, count) => {
                // 2019-03-11:华南:深圳:1, 2
                val split: Array[String] = dayAreaAdsCity.split(":")
                (s"${split(0)}:${split(1)}:${split(3)}", count)
            }
        }.reduceByKey(_ + _).map{
            // RDD[(day,(area,(adsId, count2)))]
            case (dayAreaAds, count) => {
                val split: Array[String] = dayAreaAds.split(":")
                (split(0), (split(1), (split(2), count)))
            }
        }
        
        // 2. 按照key进行分组 RDD[key, Iterator[(area,(adsId, count2))]]
        val groupByDayDStream: DStream[(String, Iterable[(String, (String, Int))])] = dayAreaAdsCountDStream.groupByKey
        // (day, Map[area, "广告1: 200, 广告2: 200"])
        val resultDStream: DStream[(String, Map[String, String])] = groupByDayDStream.map {
            case (day, it: Iterable[(String, (String, Int))]) => {
                // Map[area, Iterable[(area, (ads, count)]]
                val temp1: Map[String, Iterable[(String, (String, Int))]] = it.groupBy(_._1)
                // 去掉冗余的area
                val temp2: Map[String, Iterable[(String, Int)]] = temp1.map {
                    case (day, it) => {
                        (day, it.map(_._2))
                    }
                }
                // 每个地区的广告按照点击量取前 3
                val temp3 = temp2.map {
                    case (day, it) => {
                        val list: List[(String, Int)] = it.toList.sortWith(_._2 > _._2).take(3)
                        import org.json4s.JsonDSL._
                        val adsCountJsonString: String = JsonMethods.compact(JsonMethods.render(list))
                        (day, adsCountJsonString)
                    }
                }
                (day, temp3)
            }
        }
        
        // 3.写入的redis
        resultDStream.foreachRDD(rdd => {
            val dayAreaAdsCountArray: Array[(String, Map[String, String])] = rdd.collect
            
            val client: Jedis = RedisUtil.getJedisClient
            dayAreaAdsCountArray.foreach{
                case (day, map) => {
                    // 用来把scala的map转成java的map
                    import scala.collection.JavaConversions._
                    client.hmset("area:ads:top3:" + day, map)
                }
            }
            
            client.close()
        })
    }
}
/*
 (2019-03-11:华南:深圳:1, 2)
 RDD[2019-03-11:华南:深圳:1, 2]  把城市去掉  map
 => RDD[day:area:adsId, count1] reduceByKey
 => RDD[day:area:adsId, count2] map
 => RDD[(day,(area,(adsId, count2))] groupByKey
 => RDD[key, Iterator[(area,(adsId, count2))]]  对迭代器按照area做groupBy
 => RDD[key, Map[area, Iterator[(area,(adsId, count2))]]]  map

倒推:
=> RDD[(day, Map[area, Iterator(adsId, count)])]  把迭代器转成list, 然后排序, 然后去前3
=> RDD[(day, Map[area, List(adsId, count)])]   把list转成json格式的字符串
=> RDD[(day, Map[area, adsIdCountJsonString])]  // List((1, a), (2, b)) =>  {1:a;2:b}
 client.hmset("area:ads:top3:" + day, Map[area, adsIdCountJsonString])
 */
