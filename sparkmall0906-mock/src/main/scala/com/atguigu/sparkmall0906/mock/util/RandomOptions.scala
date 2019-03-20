package com.atguigu.sparkmall0906.mock.util

import scala.collection.mutable.ListBuffer

/**
  * 根据提供的值和比重, 来创建RandomOptions对象.
  * 然后可以通过getRandomOption来获取一个随机的预定义的值
  */
object RandomOptions {
    def apply[T](opts: (T, Int)*) ={
        val randomOptions = new RandomOptions[T]()
        randomOptions.totalWeight = (0 /: opts)(_ + _._2) // 计算出来总的比重   opts./:(0)(_ + _._2) === opts.foldLeft(0)(_ + _._2)
        opts.foreach{
            case (value, weight) => randomOptions.options ++= (1 to weight).map(_ => value)  // ::  :::  /:  :\
        }
        randomOptions
    }
    
    def main(args: Array[String]): Unit = {
        
        // 测试
//        val opts = RandomOptions(("张三", 1), ("李四", 3), ("a", 2))
//        println(opts.options)
//        (0 to 40).foreach(_ => println(opts.getRandomOption()))
        
//        val list = List((1, "a"), (3, "b"), (4, "b"),(5, "c"))
////        println(list.reduce(_ + _))
//        println(list.foldLeft(0)((x, e) => {
//            x + e._1
//        }))
//        println(list.foldLeft(0)(_ + _._1))
//
//        println((0 /: list) (_ + _._1))
    }
}
class RandomOptions[T]{
    var totalWeight: Int = _
    var options = ListBuffer[T]()  //
    
    /**
      * 获取随机的 Option 的值
      * @return
      */
    def getRandomOption() = {
        options(RandomNumUtil.randomInt(0, totalWeight - 1))
    }
}