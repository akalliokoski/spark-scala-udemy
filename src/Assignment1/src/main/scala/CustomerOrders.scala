import org.apache.spark.SparkContext

object CustomerOrders {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val id = fields(0).toInt
    val amount = fields(2).toDouble
    (id, amount)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "CustomerOrders")
    val lines = sc.textFile("../../customer-orders.csv")

    val amounts = lines
      .map(parseLine)
      .reduceByKey((x, y) => x + y)

    // sortBy could be used also as it makes intentions a little bit more obvious
    // sortByKey outputs partitioned RDD what can be useful for downstream processing
    val sorted = amounts
      .map(x => (x._2, x._1))
      .sortByKey()
      .collect()

    sorted.foreach(x => {
      val id = x._2
      val amount = x._1
      println(s"$id: $amount")
    })

  }

}
