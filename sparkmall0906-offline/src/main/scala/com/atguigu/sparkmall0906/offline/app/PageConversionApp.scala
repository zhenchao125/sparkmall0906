package com.atguigu.sparkmall0906.offline.app

import java.text.DecimalFormat

import com.atguigu.sparkmall0906.common.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConversionApp {
    /*
    统计单页跳转率
     */
    def calc(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], targetPages: String, taskId: String) = {
        // 1. 先拿到目标页面  拉链, 最后组装成想要的跳转流
        val pages: Array[String] = targetPages.split(",")
        val prePages: Array[String] = pages.slice(0, pages.length - 1) // 1 2 3 4 5 5 6  7
        val postPages: Array[String] = pages.slice(1, pages.length)
        val targetJumpPages = prePages.zip(postPages).map {
            case (p1, p2) => p1 + "->" + p2
        }
        
        // 2. 计算每个目标页面的点击次数
        // 2.1获取到包含目标页面的UserVisitAction  RDD
        val targetUserVisitAction: RDD[UserVisitAction] = userVisitActionRDD.filter(action => prePages.contains(action.page_id.toString))
        // 2.2 按照page_id计算点击的次数   reduceByKey   countByKey   (1, 10000), (2, 2000)
        val targetPagesCountMap: collection.Map[Long, Long] = targetUserVisitAction.map {
            action => (action.page_id, 1)
        }.countByKey
        
        // 3. 统计跳转流的次数
        val groupedRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)
        // "1->2"  "1->2"  "1->2"  "1->2"  "1->3"
        val jumpFlow: RDD[String] = groupedRDD.flatMap {
            case (_, it) => {
                val sortedList: List[UserVisitAction] = it.toList.sortBy(_.action_time)
                val preActions: List[UserVisitAction] = sortedList.slice(0, sortedList.length - 1)
                val postActions: List[UserVisitAction] = sortedList.slice(1, sortedList.length)
                preActions.zip(postActions).map {
                    case (action1, action2) => action1.page_id + "->" + action2.page_id
                }
            }
        }
        // 3.2 过滤出来目标的跳转流
        val targetJumpFlow: RDD[String] = jumpFlow.filter(flow => targetJumpPages.contains(flow))
        
        // 3.3 计算每个跳转流的次数   ("1->2", 100) , ("2->3", 200)    // 分母: targetPagesCountMap
        val targetJumpFlowCount: collection.Map[String, Long] = targetJumpFlow.map((_, 1)).countByKey
        // 4. 计算跳转率     ("1->2", 10.22%), ("2->3", 20.46%)
        val  formatter = new DecimalFormat("0.00%")
        val targetJumpRate: collection.Map[String, Any] = targetJumpFlowCount.map {
            case (flow, count) => {
                val key = flow.split("->")(0).toLong
                val jumpRate: Double = count.toDouble / targetPagesCountMap.getOrElse(key, 0L)
                (flow, formatter.format(jumpRate)) //
            }
        }
        println(targetJumpRate)
        // 5 写入到mysql数据库
        
        
    }
}

/*
跳转率:

1. 获取到需要计算目标页面  1,2,3,4,5,6,7
    1->2 / 1
    6->7 / 6

    1,2,3,4,5,6 页面的点击次数
        RDD[UserVisitAction].filter(page=1, click.id != -1).聚合

    针对每个 session 1->2 的调整的次数   200 / 1000

2. 得到跳转流
    "1->2"   2->3  3->4  ... 6->7
    arr = 1,2,3,4,5,6,7.split(",")
    arr1 = arr.slice(0, 5)  // 切片   1 2 3 4 5 6
    arr2 = arr.slice(1,6)  // 2,3,4,5,6,7

    arr1.zip(arr2)  ((1,2), (2,3)...)
    map(_ + "->" + _)  // ["1->2", "2->3",...]

3. RDD[UserVisitAction]  map=> RDD[sid, UserVisitAction]

groupByKey => RDD[sid, Iterator[UserVisitAction]]

4. 对象 迭代器进行按照时间进行升序排列
    Iterator[UserVisitAction] sourtBy => List[UserVisitAction]

       list1 =  list.slice(0, len-1)
       list2 =  list.slice(1, len)

       newList["1-> 2", "2->3"] = list1 zip list2 map

       map("1->2", 1)  => reduce("1->2", 1000)
*/
