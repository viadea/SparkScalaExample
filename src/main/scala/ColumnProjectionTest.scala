/* Column Projection Test */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.hadoop.fs.{FileSystem, Path}

object ColumnProjectionTest {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("ColumnProjectionTest").
                config("spark.executor.memory", "1g").
                config("spark.executor.instances", 4).
                config("spark.sql.parquet.filterPushdown",false).
                getOrCreate()

    val df = spark.read.json("/data/activity-data/")
    val targetdir = "/tmp/test_column_projection/newdf"

    println(s"====================   Writing parquet data to $targetdir    ====================")
    df.write.mode("overwrite").format("parquet").save(targetdir)

    val readdf = spark.read.format("parquet").load(targetdir)
    readdf.createOrReplaceTempView("readdf") 

    val somecols  = "SELECT Device FROM readdf WHERE Model='something_not_exist'"
    val allcols = "SELECT * FROM readdf where Model='something_not_exist'"
    
    println("====================1.  Column projection    ====================")
    println(s"====================    $somecols    ====================")
    val goodresult = spark.sql(somecols)
    goodresult.explain
    goodresult.collect

    println("====================2.  Read All Columns    ====================")
    println(s"====================    $allcols    ====================")
    val badresult = spark.sql(allcols)
    badresult.explain
    badresult.collect

    spark.stop()
  }
}
