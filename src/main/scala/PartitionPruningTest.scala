/* Partition Pruning Test */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.hadoop.fs.{FileSystem, Path}

object PartitionPruningTest {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("PartitionPruningTest").
                config("spark.executor.memory", "1g").
                config("spark.executor.instances", 4).
                getOrCreate()

    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/retail-data/by-day/*.csv")
    //Add a new column named "AnotherCountry" to have the same value as column "Country" so that we can compare the different query plan.
    val newdf = df.withColumn("AnotherCountry", expr("Country"))
    val targetdir = "/tmp/test_partition_pruning/newdf"

    println(s"====================   Writing data to $targetdir    ====================")
    newdf.write.mode("overwrite").format("orc").partitionBy("Country").save(targetdir)
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path("/tmp/test_partition_pruning/newdf")).filter(_.isDir).map(_.getPath).foreach(println)

    val readdf = spark.read.format("orc").load(targetdir)
    readdf.createOrReplaceTempView("readdf") 
    val goodsql = "SELECT * FROM readdf WHERE Country = 'Australia'"
    val badsql  = "SELECT * FROM readdf WHERE AnotherCountry = 'Australia'"
    println("====================1.  Partition Pruning is happening    ====================")
    println(s"====================    $goodsql    ====================")
    val goodresult = spark.sql(goodsql)
    goodresult.explain
    println(s"Result:   ${goodresult.count()} ")

    println("====================2.  Partition Pruning is not happening    ====================")
    println(s"====================    $badsql    ====================")
    val badresult = spark.sql(badsql)
    badresult.explain
    println(s"Result:   ${badresult.count()} ")

    spark.stop()
  }
}
