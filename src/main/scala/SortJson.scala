/* Sort a json File */
import org.apache.spark.sql.SparkSession

object SortJson {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("SortJson").getOrCreate()
    val flightData2015 = spark.read.json("/data/flight-data/json/2015-summary.json")
    val sortedFlightData2015 = flightData2015.sort("count").collect
    println(s"Output: $sortedFlightData2015")
    spark.stop()
  }
}
