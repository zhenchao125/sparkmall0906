package com.atguigu.sparkmall0906.mock.util

import java.text.SimpleDateFormat
import java.util.Date

object RandomDate {
    def apply(startDate: Date, stopDate: Date, step: Int) = {
        val randomDate = new RandomDate
        val avgStepTime = (stopDate.getTime - startDate.getTime) / step   // 10s
        randomDate.maxStepTime = 4 * avgStepTime
        randomDate.lastDateTIme = startDate.getTime
        randomDate
    }
    
    def main(args: Array[String]): Unit = {
        var dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val fromDate = dateFormat.parse("2019-03-20")
        val toDate = dateFormat.parse("2019-03-23")
        val rt = RandomDate(fromDate, toDate, 10000)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
        println(rt.getRandomDate)
    }
}

class RandomDate {
    // 上次 action 的时间
    var lastDateTIme: Long = _
    // 每次最大的步长时间
    var maxStepTime: Long = _
    
    /**
      * 得到一个随机时间
      * @return
      */
    def getRandomDate = {
        // 这次操作的相比上次的步长
        val timeStep = RandomNumUtil.randomLong(0, maxStepTime)
        lastDateTIme += timeStep
        new Date(lastDateTIme)
    }
}