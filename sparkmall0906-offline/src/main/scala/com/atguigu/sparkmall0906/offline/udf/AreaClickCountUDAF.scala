package com.atguigu.sparkmall0906.offline.udf

import java.text.DecimalFormat

import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AreaClickCountUDAF extends UserDefinedAggregateFunction {
    // 输入的数据类型 String
    override def inputSchema: StructType = {
        StructType(StructField("city_name", StringType) :: Nil)
        //        StructType(Array(StructField("city_name", StringType)))
    }
    
    // 存储数据的类型  Map  Long
    override def bufferSchema: StructType = {
        StructType(StructField("city_count", MapType(StringType, LongType)) :: StructField("total_count", LongType) :: Nil)
    }
    
    // 输出的类型  String
    override def dataType: DataType = StringType
    
    // 相同的输入是否应该返回相同的输出
    override def deterministic: Boolean = true
    
    // 给存储数据进行初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 存储 map 和 Long
        // 初始化Map
        buffer(0) = Map[String, Long]()
        // 初始化 计算式总的点击
        buffer(1) = 0L
    }
    
    // 分区内合并操作  executor内合并
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 北京
        val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
        val cityName: String = input.getString(0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
        // 总量总是 +1
        buffer(1) = buffer.getLong(1) + 1L
        //        buffer(1) = buffer.getAs[Long](1) + 1L
    }
    
    // 分区间的合并  executor间合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        // 两个map的合并.  map1 map2     map1 ++ map2
        val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
        val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
        buffer1(0) = map1.foldLeft(map2) {
            case (m, (k, v)) => {
                m + (k -> (m.getOrElse(k, 0L) + v))
            }
        }
        // 总量的合并
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    
    
    // 输出  把存储的数据转换成字符串输出
    override def evaluate(buffer: Row): Any = {
        val cityCount = buffer.getAs[Map[String, Long]](0)
        val totalCount = buffer.getLong(1)
        val cityRemarks: List[CityRemark] = cityCount.toList
            .sortBy(_._2)(Ordering.Long.reverse)
            .take(2)
            .map {
                case (city, count) => {
                    CityRemark(city, count.toDouble / totalCount)
                }
            }
        
        val allRemarks = cityRemarks :+ CityRemark("其他", cityRemarks.foldLeft(1D)(_ - _.clickRatio))
        // 拼成字符串返回
        allRemarks.mkString(",")
    }
}

case class CityRemark(cityName: String, clickRatio: Double) {
    val formatter = new DecimalFormat("0.00%")
    
    override def toString: String = s"$cityName:${formatter.format(clickRatio)}"
}

/*
1. 输入是什么
    城市名    String

2. 中间需要存储什么
    华北  商品1
     - 每个城市的点击量  map
        北京: 1000
        天津: 500
     - 这个地区的某个商品的总点击量  Long
         商品1的总点击量 10000

3. 输出是什么
    城市和城市的比例  String


 */