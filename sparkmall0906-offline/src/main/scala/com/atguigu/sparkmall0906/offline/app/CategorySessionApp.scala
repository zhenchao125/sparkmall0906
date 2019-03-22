package com.atguigu.sparkmall0906.offline.app

import com.atguigu.sparkmall0906.common.bean.UserVisitAction
import com.atguigu.sparkmall0906.common.util.JDBCUtil
import com.atguigu.sparkmall0906.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategorySessionApp {
    def statCategoryTop10Session(spark: SparkSession, CategoryCountInfoList: List[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction], taskId: String) ={
        //1. 需求1: Category Top  只需要拿出来 CategoryId
        val top10CategoryId: List[String] = CategoryCountInfoList.map(_.categoryId)
        //2. 从 RDD[UserVisitAction].filter(cids.contains(_.CategoryId))   只有top10 品类的用户行为日志  =>
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(userAction => {
            top10CategoryId.contains(userAction.click_category_id.toString)
        })
        //3. 类型的转换
        val categoryCountRDD: RDD[(Long, (String, Int))] = filteredUserVisitActionRDD.map {
            action => ((action.click_category_id, action.session_id), 1)
        }.reduceByKey(_ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        // 4. 按照key进行分组
        val groupdCategoryCountRDD: RDD[(Long, Iterable[(String, Int)])] = categoryCountRDD.groupByKey
        val categorySessionRDD: RDD[CategorySession] = groupdCategoryCountRDD.flatMap {
            // 5. 对象迭代器中的数据排序, 然后去前10
            case (cid, it) => {
                it.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).map {
                    case (sid, count) => CategorySession(taskId, cid.toString, sid, count)
                }
            }
        }
        // 5. 写入到mysql  数据量已经很小, 可以数据拉到 Driver, 然后再写入
    
        val csArray = categorySessionRDD.map(cs => Array(cs.taskId, cs.categoryId, cs.sessionId, cs.clickCount)).collect
        JDBCUtil.executeUpdate("truncate table category_top10_session_count", null)
        JDBCUtil.executeBatchUpdate("insert into category_top10_session_count values(?, ?, ?, ?)", csArray)
    }
}

/*

1. 需求1: Category Top  只需要拿出来 CategoryId

2. 从 RDD[UserVisitAction].filter(cids.contains(_.CategoryId))   只有top10 品类的用户行为日志  =>

3. RDD[UserVisitAction].map(action => ((cid, sid), 1)) => //key: (cid, sid)  value: 1

4. RDD.reduceByKey  => ((cid, sid), count)


// 10
5. RDD((cid, sid), count)  => RDD(cid, (sid, count))  => groupByKey(cid, Iterable[(sid, count)])

=> flapMap()  // 排序, 取前10

6. 写到数据库
 */