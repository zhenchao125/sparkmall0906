package com.atguigu.sparkmall0906.mock

import java.text.SimpleDateFormat
import java.util.UUID

import com.atguigu.sparkmall0906.common.bean.{CityInfo, ProductInfo, UserInfo, UserVisitAction}
import com.atguigu.sparkmall0906.common.util.ConfigurationUtil
import com.atguigu.sparkmall0906.mock.util.{RandomDate, RandomNumUtil, RandomOptions}
import com.atguigu.sparkmall0906.common.util.ConfigurationUtil
import com.atguigu.sparkmall0906.mock.util.RandomOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 生成离线数据
  */
object MockOffline {
    // --- 用户相关参数 开始 ---
    // 用户数量
    val userNum = 100
    // 年龄范围
    val ageFrom = 20
    val ageTo = 60
    // 职业
    val professionOpts = RandomOptions(("学生", 4), ("程序员", 3), ("经理", 2), ("老师", 1))
    // 性别
    val genderOpts = RandomOptions(("男", 6), ("女", 4))
    // --- 用户相关参数 结束 ---
    
    // --- 商品相关参数 开始 ---
    //品类数量
    val categoryNum = 20
    // 商品数量
    val productNum = 100
    // --- 商品相关参数 结束 ---
    
    
    // --- 用户行为相关参数 开始 ---
    // 搜索关键词
    val searchKeywordsOpts = RandomOptions(("手机", 30), ("笔记本", 70), ("内存", 70), ("i7", 70), ("苹果", 70), ("吃鸡", 70))
    // 动作
    val actionOpts = RandomOptions(("search", 20), ("click", 60), ("order", 6), ("pay", 4), ("quit", 10))
    // session 数量
    val sessionNum = 10000
    //系统页面数
    val pageNum = 50
    // 日志大致数量，用于分布时间
    val logAboutNum = 100000
    // --- 用户行为相关参数 结束 ---
    
    
    /**
      * 生成 UserInfo 数据
      *
      * @return 所有的 UserInfo 组成的 List 集合
      */
    def mockUserInfo: List[UserInfo] = {
        (1 to userNum).toList.map(
            i => UserInfo(i,
                s"user_$i",
                s"name_$i",
                RandomNumUtil.randomInt(ageFrom, ageTo),
                professionOpts.getRandomOption(),
                genderOpts.getRandomOption()
            ))
    }
    
    /**
      * 生成 CityInfo 数据
      *
      * @return 所有的 CityInfo 组成的 List 集合
      */
    def mockCityInfo: List[CityInfo] = {
        List(CityInfo(1L, "北京", "华北"),
            CityInfo(2L, "上海", "华东"),
            CityInfo(3L, "深圳", "华南"),
            CityInfo(4L, "广州", "华南"),
            CityInfo(5L, "武汉", "华中"),
            CityInfo(6L, "南京", "华东"),
            CityInfo(7L, "天津", "华北"),
            CityInfo(8L, "成都", "西南"),
            CityInfo(9L, "哈尔滨", "东北"),
            CityInfo(10L, "大连", "东北"),
            CityInfo(11L, "沈阳", "东北"),
            CityInfo(12L, "西安", "西北"),
            CityInfo(13L, "长沙", "华中"),
            CityInfo(14L, "重庆", "西南"),
            CityInfo(15L, "济南", "华东"),
            CityInfo(16L, "石家庄", "华北"),
            CityInfo(17L, "银川", "西北"),
            CityInfo(18L, "杭州", "华东"),
            CityInfo(19L, "保定", "华北"),
            CityInfo(20L, "福州", "华南"),
            CityInfo(21L, "贵阳", "西南"),
            CityInfo(22L, "青岛", "华东"),
            CityInfo(23L, "苏州", "华东"),
            CityInfo(24L, "郑州", "华北"),
            CityInfo(25L, "无锡", "华东"),
            CityInfo(26L, "厦门", "华南"))
    }
    
    /**
      * 生成 ProductInfo 数据
      *
      * @return 所有的 ProductInfo 对象组成的 List 集合
      */
    def mockProductInfo: List[ProductInfo] = {
        
        // 商品的品类
        val productExtendOpts = RandomOptions(("自营", 70), ("第三方", 30))
        (1 to productNum).toList.map(
            i => ProductInfo(i, s"商品_$i", productExtendOpts.getRandomOption())
        )
    }
    
    /**
      * 生成 UserVisitAction 数据
      */
    def mockUserVisitAction: List[UserVisitAction] = {
        val dateFormatter = new SimpleDateFormat("yyy-MM-dd")
        val timeFormatter = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")
        // 开始日期
        val fromDate = dateFormatter.parse("2019-03-20")
        // 结束日期
        val toDate = dateFormatter.parse("2019-03-24")
        
        
        val randomDate = RandomDate(fromDate, toDate, logAboutNum)
        val rows = ListBuffer[UserVisitAction]()
        // 根据 session 来创建对应 action
        for (i <- 1 to sessionNum) {
            val userId = RandomNumUtil.randomInt(1, userNum)
            val sessionId = UUID.randomUUID().toString
            var isQuit = false
            while (!isQuit) {
                val action = actionOpts.getRandomOption()
                if (action == "quit") {
                    isQuit = true
                } else {
                    val date = randomDate.getRandomDate
                    // 2019-03-20
                    val actionDateString = dateFormatter.format(date)
                    // 2019-03-20 01:01:01
                    val actionTimeString = timeFormatter.format(date)
                    
                    var searchKeyword: String = null
                    var clickCategoryId: Long = -1
                    var clickProductId: Long = -1
                    var orderCategoryIds: String = null
                    var orderProductIds: String = null
                    var payCategoryIds: String = null
                    var payProductIds: String = null
                    
                    val cityId: Long = RandomNumUtil.randomLong(1, 26)
                    action match {
                        case "search" => searchKeyword = searchKeywordsOpts.getRandomOption()
                        case "click" => {
                            clickCategoryId = RandomNumUtil.randomInt(1, categoryNum)
                            clickProductId = RandomNumUtil.randomInt(1, productNum)
                        }
                        case "order" => {
                            orderCategoryIds = RandomNumUtil.randomMultiInt(1, categoryNum, RandomNumUtil.randomInt(1, 3), false).mkString(",")
                            orderProductIds = RandomNumUtil.randomMultiInt(1, productNum, RandomNumUtil.randomInt(1, 5), false).mkString(",")
                        }
                        case "pay" => {
                            payCategoryIds = RandomNumUtil.randomMultiInt(1, categoryNum, RandomNumUtil.randomInt(1, 3), false).mkString(",")
                            payProductIds = RandomNumUtil.randomMultiInt(1, productNum, RandomNumUtil.randomInt(1, 5), false).mkString(",")
                        }
                    }
                    rows += UserVisitAction(actionDateString,
                        userId,
                        sessionId,
                        RandomNumUtil.randomInt(1, pageNum),
                        actionTimeString,
                        searchKeyword,
                        clickCategoryId,
                        clickProductId,
                        orderCategoryIds,
                        orderProductIds,
                        payCategoryIds,
                        payProductIds,
                        cityId)
                }
            }
        }
        rows.toList
    }
    
    def main(args: Array[String]): Unit = {
        // 模拟数据
        val userVisitActionData = mockUserVisitAction
        val userInfoData = mockUserInfo
        val productInfoData = mockProductInfo
        val cityInfoData = mockCityInfo
        
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("MockOffline")
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", "hdfs://hadoop201:9000/user/hive/warehouse")
            .getOrCreate()
        
        import spark.implicits._
        val sc = spark.sparkContext
        val userVisitActionDF = sc.makeRDD(userVisitActionData).toDF
        val userInfoDF = sc.makeRDD(userInfoData).toDF
        val productInfoDF = sc.makeRDD(productInfoData).toDF
        val cityInfoDF = sc.makeRDD(cityInfoData).toDF
        
        insertIntoHive(spark, "user_visit_action", userVisitActionDF)
        insertIntoHive(spark, "user_info", userInfoDF)
        insertIntoHive(spark, "product_info", productInfoDF)
        insertIntoHive(spark, "city_info", cityInfoDF)
        
    }
    
    /**
      * 把数据插入到 Hive 表中
      *
      * @param spark
      * @param tableName
      * @param df
      * @return
      */
    def insertIntoHive(spark: SparkSession, tableName: String, df: DataFrame) = {
        val database = ConfigurationUtil("config.properties").getString("hive.database")
        spark.sql(s"use $database") // 切换数据库
        spark.sql(s"drop table if exists $tableName") // 如果表已经存在, 则删除该表
        df.write.saveAsTable(tableName) // 保存数据
        spark.sql(s"select * from $tableName").show(100)
        println(s"$tableName 数据写入完毕!")
    }
}
