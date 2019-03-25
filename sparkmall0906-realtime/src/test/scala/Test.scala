object Test {
    def main(args: Array[String]): Unit = {
        val list = List(("a", 1), ("b", 2), ("a", 3))
    
        println(list.groupBy(_._1))
    }
}
