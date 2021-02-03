/* Predicate PushDown Test */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.hadoop.fs.{FileSystem, Path}

object PredicatePushdownTest {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("PredicatePushdownTest").
                config("spark.executor.memory", "1g").
                config("spark.executor.instances", 4).
                config("spark.sql.parquet.filterPushdown",true).
                getOrCreate()

    val targetdir = "/tmp/test_column_projection/newdf"
    val readdf = spark.read.format("parquet").load(targetdir)
    readdf.createOrReplaceTempView("readdf") 

    val q1  = "SELECT * FROM readdf WHERE Index=20000"
    val q2   = "SELECT * FROM readdf where Index=9999999999"
    
    println(s"====================1.  $q1    ====================")
    val result1 = spark.sql(q1)
    result1.explain
    result1.collect

    println(s"====================2.  $q2    ====================")
    val result2 = spark.sql(q2)
    result2.explain
    result2.collect

    spark.stop()
  }
}
