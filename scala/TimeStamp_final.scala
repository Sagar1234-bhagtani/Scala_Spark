import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object TimeStamp_final {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()


    var dataSet = spark.read.format("com.crealytics.spark.excel")
      .option("header", "true")
      //.option("dataAddress","Sheet1")
      .option("maxRowsInMemory", 500)
      //.option("InferSchema", "True")
      .load("C:/Users/sagar/Videos/Captures/poplu.xlsx")

    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer()

    dataSet.dtypes.foreach(f => arrayBuffer1 += f._1.toString())
    arrayBuffer1.foreach(r => print(r))
    dataSet.printSchema()

  }
}
