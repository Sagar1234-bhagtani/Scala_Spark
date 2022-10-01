import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

import scala.collection.mutable.ArrayBuffer

object Tassk_1 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()


    val dataSet = spark.read.format("com.crealytics.spark.excel")
      .option("header", "true")
      //.option("dataAddress","Sheet1")
      .option("maxRowsInMemory", 500)

      .option("InferSchema", "True")
      .load("C:/Users/sagar/Videos/Captures/Book2.xlsx")

    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer()

    dataSet.dtypes.foreach(f=>arrayBuffer1+=f._1.toString())
    //dataSet.select("Name").show()
    arrayBuffer1.foreach(r=>print(r))
    for(i<-0 to arrayBuffer1.size-1){

      val df9=dataSet.select(s"${arrayBuffer1(i).toString()}")

      df9.foreach { row =>
        row.toSeq.foreach { col => println(col) }
      }


    }



  }
}