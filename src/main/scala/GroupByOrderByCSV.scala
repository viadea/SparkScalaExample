/* Group by & Order By on a CSV file in SQL way and DataFrame way  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object GroupByOrderByCSV {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("GroupByCSV").getOrCreate()
    val flightData2015 = spark.read.option("inferSchema","true").option("header", "true").csv("/data/flight-data/csv/2015-summary.csv")

    // SQL way
    flightData2015.createOrReplaceTempView("flight_data_2015")
    val sqlWay = spark.sql(""" SELECT DEST_COUNTRY_NAME, sum(count) as destination_total FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME ORDER BY sum(count) DESC limit 5   """)
    println("====================   SQL way output:   ====================")
    sqlWay.show()

    // DataFrame way
    val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME")
                                     .sum("count")
                                     .withColumnRenamed("sum(count)", "destination_total")
                                     .sort(desc("destination_total"))
                                     .limit(5)

    println("====================   DataFrame way output:   ====================")
    dataFrameWay.show()

    spark.stop()
  }
}
