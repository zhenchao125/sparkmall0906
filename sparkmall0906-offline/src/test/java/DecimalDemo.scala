import java.text.DecimalFormat

object DecimalDemo {
    def main(args: Array[String]): Unit = {
        //        val  formatter = new DecimalFormat("0.00%")
        //        println(formatter.format(0.26735555))
        
        val map1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
        val map2 = Map("a" -> 10, "d" -> 30, "b"-> 20)
        
        // a->11  b->2  c->3  d->30
        /*val map3: Map[String, Int] = map1 ++ map2.map {
            case (k, v) => (k, map1.getOrElse(k, 0) + v)
        }*/
        /*val map3: Map[String, Int] = map1.foldLeft(map2) {
            case (m, (k, v)) => {
                m + (k -> (m.getOrElse(k, 0) + v))
            }
        }
        */
        val map3: Map[String, Int] = (map2 /: map1) {
            case (m, (k, v)) => {
                m + (k -> (m.getOrElse(k, 0) + v))
            }
        }
        println(map3)
    }
}
