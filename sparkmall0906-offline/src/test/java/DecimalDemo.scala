import java.text.DecimalFormat

object DecimalDemo {
    def main(args: Array[String]): Unit = {
        val  formatter = new DecimalFormat("0.00%")
        println(formatter.format(0.26735555))
    }
}
