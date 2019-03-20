package com.atguigu.sparkmall0906.offline

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall0906.common.bean.UserVisitAction
import com.atguigu.sparkmall0906.common.util.ConfigurationUtil
import com.atguigu.sparkmall0906.offline.app.CategoryTop10App
import com.atguigu.sparkmall0906.offline.bean.Condition
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
        // 根据条件过滤出来需要的 UserVisitAction
        val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(spark, readCondition())
        
        // 需求1: 统计品类的top10
        CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD)
        
    }
    /**
      * 根据传入的条件, 来读取用户行为的数据
      *
      * @param spark
      * @param condition
      */
    def readUserVisitActionRDD(spark: SparkSession, condition: Condition): RDD[UserVisitAction] = {
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
        spark.sql(sql).as[UserVisitAction].rdd
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
