package Others

import org.apache.spark.sql.SparkSession

object Time_stamp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("SparkByExamples.com")
      .getOrCreate()


    val dataSet = spark.read.format("com.crealytics.spark.excel")
      .option("header", "true")
      //.option("dataAddress","Sheet1")
      .option("maxRowsInMemory", 500)

      .option("InferSchema", "True")
      .load("C:/Users/sagar/Videos/Captures/OnlineRetail.xlsx")

dataSet.printSchema()
    dataSet.show()

  }

}
