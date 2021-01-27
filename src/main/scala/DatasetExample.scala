/* Dataset Example  */
import org.apache.spark.sql.SparkSession

case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

object DatasetExample {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("GroupByCSV").getOrCreate()
    import spark.implicits._
    
    val flightsDF = spark.read.parquet("/data/flight-data/parquet/2010-summary.parquet/")
    val flights = flightsDF.as[Flight]

    flights.filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
           .map(flight_row => flight_row)
           .show(5)

    flights.take(5)
           .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
           .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))
           .toSeq
           .toDS
           .show()

    spark.stop()
  }
}
