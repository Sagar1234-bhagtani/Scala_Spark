package Others

import org.apache.spark.sql.SparkSession

object cHECKING_tIME {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    val t1 = System.nanoTime()


    //    var dataSet = spark.read.format("com.crealytics.spark.excel")
    //      .option("header", "true")
    //      //.option("dataAddress","Sheet1")
    //      .option("maxRowsInMemory", 500)
    //      //.option("InferSchema", "True")
    //      .load("C:/Users/sagar/Videos/Captures/OnlineRetail.xlsx")


    var dataSet = spark.read
      .format("com.crealytics.spark.excel")
      //.option("dataAddress", "0!A1")
      .option("header", "true")
      //.option("treatEmptyValuesAsNulls", "true")
      //.option("setErrorCellsToFallbackValues", "true")
      //.option("usePlainNumberFormat", "true")
      .option("inferSchema", "true")

      .load("C:/Users/sagar/Videos/Captures/part1.xls")

print((System.nanoTime()-t1)/1000000)
    dataSet.printSchema()
  }
}
