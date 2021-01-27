/* Group by on a CSV file in SQL way and DataFrame way  */
import org.apache.spark.sql.SparkSession

object GroupByCSV {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("GroupByCSV").getOrCreate()
    val flightData2015 = spark.read.option("inferSchema","true").option("header", "true").csv("/data/flight-data/csv/2015-summary.csv")

    // SQL way
    flightData2015.createOrReplaceTempView("flight_data_2015")
    val sqlWay = spark.sql(""" SELECT DEST_COUNTRY_NAME, count(1) FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME """)
    println("====================   SQL way output:   ====================")
    sqlWay.collect().foreach(println)

    // DataFrame way
    val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
    println("====================   DataFrame way output:   ====================")
    sqlWay.collect().foreach(println)

    spark.stop()
  }
}
