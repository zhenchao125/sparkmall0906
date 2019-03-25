package com.atguigu.sparkmall0906.offline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall0906.common.bean.UserVisitAction
import com.atguigu.sparkmall0906.common.util.ConfigurationUtil
import com.atguigu.sparkmall0906.offline.app.{AreaClickApp, CategorySessionApp, CategoryTop10App, PageConversionApp}
import com.atguigu.sparkmall0906.offline.bean.{CategoryCountInfo, Condition}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OfflineApp {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        // 1. 把用户行为的数据读取出来, 放在 RDD 中
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("MockOffline")
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", "hdfs://hadoop201:9000/user/hive/warehouse")
            .getOrCreate()
        val taskId: String = UUID.randomUUID().toString
        // 根据条件过滤出来需要的 UserVisitAction
        val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(spark, readCondition())
        userVisitActionRDD.cache //做缓存
//        userVisitActionRDD.persist()
        // 需求1: 统计品类的top10
//        val categoryTop10: List[CategoryCountInfo] = CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD, taskId)
        // 需求2: 统计top10品类的 top10 活跃 session
//        CategorySessionApp.statCategoryTop10Session(spark, categoryTop10, userVisitActionRDD, taskId)
        
        // 需求3: 统计 单页跳转率
//        PageConversionApp.calc(spark, userVisitActionRDD, readCondition().targetPageFlow, taskId)
        // 需求4: 统计地区商品的top3
        AreaClickApp.statAreaClickTop3Product(spark, taskId)
    }
    
    /**
      * 根据传入的条件, 来读取用户行为的数据
      *
      * @param spark
      * @param condition
      */
    def readUserVisitActionRDD(spark: SparkSession, condition: Condition) = {
        // 1. 先有sql语句
        var sql =
            s"""
               |select
               |    v.*
               |from user_visit_action v join user_info u on v.user_id=u.user_id
               |where 1=1
            """.stripMargin
        if (isNotEmpty(condition.startDate)) {
            sql += s" and v.date >= '${condition.startDate}'"
        }
        if (isNotEmpty(condition.endDate)) {
            sql += s" and v.date <= '${condition.endDate}'"
        }
        if (condition.startAge > 0) {
            sql += s" and u.age >= ${condition.startAge}"
        }
        if (condition.endAge > 0) {
            sql += s" and u.age <= ${condition.endAge}"
        }
        // 2. 执行
        import spark.implicits._
        spark.sql("use sparkmall0906")
        
        spark.sql(sql).as[UserVisitAction].rdd.map {
            action => {
                println("a");
                action
            }
        }
        
    }
    
    /**
      * 读取过滤条件
      *
      * @return
      */
    def readCondition(): Condition = {
        val conditionStr: String = ConfigurationUtil("conditions.properties").getString("condition.params.json")
        JSON.parseObject(conditionStr, classOf[Condition])
    }
}
