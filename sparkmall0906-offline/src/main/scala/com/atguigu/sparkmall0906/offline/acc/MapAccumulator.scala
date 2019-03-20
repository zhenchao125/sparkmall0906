package com.atguigu.sparkmall0906.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/*
输入类型:   (品类id, action)   => (品类1, "click")
输出:
    Map[(品类1, "click"), 100]
 */
class MapAccumulator extends AccumulatorV2[(String, String), mutable.Map[((String, String)), Long]] {
    private val map: mutable.Map[(String, String), Long] = mutable.Map[((String, String)), Long]()
    
    // 是否为空
    override def isZero: Boolean = map.isEmpty
    
    // copy数据
    override def copy(): AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] = {
        val newAcc = new MapAccumulator
        map.synchronized {
            newAcc.map ++= map
        }
        newAcc
    }
    
    // 重置
    override def reset(): Unit = map.clear()
    
    // 累加   (品类1, "click")
    override def add(v: (String, String)): Unit = {
        map(v) = map.getOrElse(v, 0L) + 1L
    }
    
    // 合并
    //  this.map    (商品1, "click") -> 100
    //  other.map   (商品1, "click") -> 200
    //  (商品1, "order") -> 200
    override def merge(other: AccumulatorV2[(String, String), mutable.Map[(String, String), Long]]): Unit = {
        other.value.foreach {
            case (k, count) => {
                map.put(k, map.getOrElse(k, 0L) + count)
            }
        }
    }
    
    // 最终的返回值
    override def value: mutable.Map[(String, String), Long] = map
}
