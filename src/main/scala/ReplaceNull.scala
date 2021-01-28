/* Replacing NULL Example  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, LongType, BooleanType}

object ReplaceNull {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("ReplaceNull").getOrCreate()

    val simpleData = Seq(Row("Jim","","Green",33333,3000.12,19605466456L,true),
        Row("Tom","A","Smith",44444,4000.45,19886546456L,null),
        Row("Jerry ",null,"Brown",null,5000.67,null,false),
        Row("Henry ","B","Jones",66666,null,20015464564L,true)
       )
    
    val simpleSchema = StructType(Array(
        StructField("firstname",StringType,true),
        StructField("middlename",StringType,true),
        StructField("lastname",StringType,true),
        StructField("zipcode", IntegerType, true),
        StructField("salary", DoubleType, true),
        StructField("account", LongType, true),
        StructField("isAlive", BooleanType, true)
      ))
    
    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),simpleSchema)
    
    println("====================   Sample schema and data source    ====================")
    df.printSchema()
    df.show()
    
    // Replace Null in ALL numeric columns.
    // Here including IntegerType, DoubleType, LongType.
    println("====================   1. Replace Null in ALL numeric columns   ====================")
    df.na.fill(0).show()
    
    // Replaces Null in specified numeric columns.
    println("====================   2. Replace Null in specified numeric column(s) --account   ====================")
    df.na.fill(0,Array("account")).show()
    
    // Replace Null in ALL string columns.
    println("====================   3. Replace Null in ALL string column(s)   ====================")
    df.na.fill("").show()
    
    // Replace Null in ALL boolean columns.
    println("====================   4. Replace Null in ALL boolean column(s)   ====================")
    df.na.fill(true).show()

    spark.stop()
  }
}


