package com.atguigu.sparkmall0906.offline.app

import com.atguigu.sparkmall0906.common.bean.UserVisitAction
import com.atguigu.sparkmall0906.offline.acc.MapAccumulator
import com.atguigu.sparkmall0906.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryTop10App {
    def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction]) = {
        // 1. 注册累加器
        val acc = new MapAccumulator
        spark.sparkContext.register(acc, "MapAccumulator")
        
        // 2. 使用累加器进行累加
        userVisitActionRDD.foreach {
            case visitAction => {
                if (visitAction.click_category_id != -1) {
                    acc.add((visitAction.click_category_id.toString, "click"))
                } else if (visitAction.order_category_ids != null) { // 下单
                    // 1,2,3
                    visitAction.order_category_ids.split(",").foreach {
                        case cid => acc.add((cid, "order"))
                    }
                    
                } else if (visitAction.pay_category_ids != null) { // 支付
                    visitAction.pay_category_ids.split(",").foreach {
                        case cid => acc.add((cid, "pay"))
                    }
                }
            }
        }
        // 累加器中的数据: Map((1, "click")-> 1000, (1, "order") -> 500, (2, "click") -> 5000, (2, "pay") -> 100)
        // 3. 分组
        val categoryCountMap: mutable.Map[(String, String), Long] = acc.value
        // 按照品类id进行分组
        val actionCountByCategoryIdMap: Map[String, mutable.Map[(String, String), Long]] = categoryCountMap.groupBy(_._1._1)
        // 4. 类型转换
        val CategoryCountInfoList: List[CategoryCountInfo] = actionCountByCategoryIdMap.map {
            case (cid, actionMap) => {
                CategoryCountInfo(
                    "",
                    cid,
                    actionMap.getOrElse((cid, "click"), 0),
                    actionMap.getOrElse((cid, "order"), 0),
                    actionMap.getOrElse((cid, "pay"), 0))
            }
        }.toList
        // 5. 排序, 取前10
        val top10: List[CategoryCountInfo] = CategoryCountInfoList.sortBy(info =>
            (info.clickCount, info.orderCount, info.payCount))(Ordering.Tuple3(Ordering.Long.reverse, Ordering.Long.reverse, Ordering.Long.reverse)).take(10)
        top10.foreach(println)
        
        // 6. 写到mysql
    }
    
}

/*
1. 遍历全部的行为日志, 根据品类id和行为的类型统计他们的次数
    品类1:  click   100
            order  50
            pay   20
            
2. 排序, 取前10

3. 写入到mysql

 */
