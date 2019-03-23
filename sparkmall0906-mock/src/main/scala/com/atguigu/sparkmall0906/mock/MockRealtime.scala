package com.atguigu.sparkmall0906.mock

import java.util.Properties

import com.atguigu.sparkmall0906.common.bean.CityInfo
import com.atguigu.sparkmall0906.mock.util.{RandomNumUtil, RandomOptions}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer

/**
  * 生成实时的模拟数据
  */
object MockRealTime {
    /*
    数据格式:
        timestamp   area    city    userid  adid
        某个时间点 某个地区  某个城市   某个用户  某个广告

     */
    def mockRealTimeData() = {
        // 存储模拟的实时数据
        val array = ArrayBuffer[String]()
        // 城市信息
        val randomOpts = RandomOptions(
            (CityInfo(1, "北京", "华北"), 30),
            (CityInfo(2, "上海", "华东"), 30),
            (CityInfo(3, "广州", "华南"), 10),
            (CityInfo(4, "深圳", "华南"), 20),
            (CityInfo(4, "杭州", "华中"), 10))
        (1 to 50).foreach {
            i => {
                val timestamp = System.currentTimeMillis()
                val cityInfo = randomOpts.getRandomOption()
                val area = cityInfo.area
                val city = cityInfo.city_name
                val userid = RandomNumUtil.randomInt(100, 105)
                val adid = RandomNumUtil.randomInt(1, 5)
                array += s"$timestamp,$area,$city,$userid,$adid"
                Thread.sleep(10)
            }
        }
        array
    }
    
    def createKafkaProducer: KafkaProducer[String, String] = {
        val props = new Properties
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
        // 等待所有副本节点的应答
        props.put("acks", "1")
        // 重试最大次数
        props.put("retries", "0")
        
        // 批消息处理大小
        props.put("batch.size", "16384")
        // 请求延时
        props.put("linger.ms", "1")
        //// 发送缓存区内存大小
        props.put("buffer.memory", "33554432")
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        new KafkaProducer[String, String](props)
    }
    
    def main(args: Array[String]): Unit = {
        val topic = "ads_log0906"
        val producer: KafkaProducer[String, String] = createKafkaProducer
        while (true) {
            mockRealTimeData().foreach {
                msg => {
                    producer.send(new ProducerRecord(topic, msg))
                    Thread.sleep(100)
                }
            }
            Thread.sleep(1000)
        }
    }
}
