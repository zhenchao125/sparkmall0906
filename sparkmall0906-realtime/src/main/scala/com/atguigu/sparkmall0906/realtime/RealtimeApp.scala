package com.atguigu.sparkmall0906.realtime

import com.atguigu.sparkmall0906.common.util.MyKafkaUtil
import com.atguigu.sparkmall0906.realtime.bean.AdsInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

object RealtimeApp {
    def main(args: Array[String]): Unit = {
        // 从kafka中读出我们需要数据
        // 1. 创建 SparkConf 对象
        val conf: SparkConf = new SparkConf()
            .setAppName("RealTimeApp")
            .setMaster("local[*]")
        // 2. 创建 SparkContext 对象
        val sc = new SparkContext(conf)
        // 3. 创建 StreamingContext
        val ssc = new StreamingContext(sc, Seconds(2))
        // 4. 得到 DStream
        val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log0906")
    
        // 5. 为了方便后面的计算, 把消费到的字符串封装到对象中
        val adsInfoDStream: DStream[AdsInfo] = recordDStream.map {
            record =>
                val split: Array[String] = record.value.split(",")
                AdsInfo(split(0).toLong, split(1), split(2), split(3), split(4))
        }
        
        // 6: 需求5:
        val filteredDStream: DStream[AdsInfo] = BlackListApp.filterBlackList(adsInfoDStream, sc)
        BlackListApp.checkUserToBlackList(filteredDStream)
        
        // 7. 需求6:
        filteredDStream.foreachRDD{
            rdd => {
                rdd.foreach{
                    info => println(info.userId)
                }
            }
        }
        ssc.start()
        ssc.awaitTermination()
    }
}
