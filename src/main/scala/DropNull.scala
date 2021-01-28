/* Drop NULL Example  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, LongType, BooleanType}

object DropNull {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("DropNull").getOrCreate()

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

    // Drop rows containing NULL in any columns.
    // Here only one row does not have NULL in any columns.
    println("====================   1. Drop rows containing NULL in any columns.(version 1)   ====================")
    df.na.drop().show()

    // Drop rows containing NULL in any columns.
    // Same as above. This is just another version.
    println("====================   2. Drop rows containing NULL in any columns.(version 2)   ====================")
    df.na.drop("any").show()

    // Drop rows containing NULL in all columns.
    // Here it shows all rows because there is no such all-NULL rows.
    println("====================   3. Drop rows containing NULL in all columns.   ====================")
    df.na.drop("all").show()

    // Drop rows containing NULL in any of specified column(s).
    println("====================   4. Drop rows containing NULL in any of specified column(s).   ====================")
    df.na.drop(Seq("salary","account")).show()

    // Drop rows containing NULL in all of specified column(s).
    println("====================   5. Drop rows containing NULL in all of specified column(s).   ====================")
    df.na.drop("all",Seq("salary","account")).show()

    // Drop rows containing less than minNonNulls non-null values. 
    // It means we keep the rows with at least minNonNulls non-null values.
    println("====================   6. Drop rows containing less than minNonNulls non-null values.   ====================")
    df.na.drop(7).show()

    spark.stop()
  }
}
