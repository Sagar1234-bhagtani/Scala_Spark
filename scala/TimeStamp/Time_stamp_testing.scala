package TimeStamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}

import util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

object Time_stamp_testing {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    var m = 0

    var dataSet = spark.read
      .format("com.crealytics.spark.excel")

      .option("header", "true")
      //.option("treatEmptyValuesAsNulls", "true")
      //.option("setErrorCellsToFallbackValues", "true")
      //.option("usePlainNumberFormat", "true")
      .option("inferSchema", "true")

      .load("C:/Users/sagar/Videos/Captures/part1.xls")
    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer()

    dataSet.dtypes.foreach(f => arrayBuffer1 += f._1)
    arrayBuffer1.foreach(r => print(r))


    dataSet.printSchema()

    for (i <- 0 to arrayBuffer1.size - 1) {
      var kk = "temp" + i
      try {
        var df9 = dataSet.select(s"${arrayBuffer1(i)}")
        var gg = df9.withColumn(s"$kk",
          to_timestamp(col(s"${arrayBuffer1(i)}"), "MM-dd-yyyy HH mm ss SSS"))
        gg.show()

        // var gg=  df9.withColumn("datetype", to_date(col(s"${arrayBuffer1(i)}")))

        gg.show()

        //dd-MM-yyyy HH mm
        df9 = gg
        //val df9 = dataSet.select(s"${arrayBuffer1(i)}")

        dataSet.printSchema()
        //      df9.foreach { row =>
        //        row.toSeq.foreach { col =>
        //          //IMPORTANT AREA
        //          println(col)
        //
        //
        //
        //        }
        //        }
        df9.show()
      }
      catch {
        case e: Exception => println("wefergweg")
      }
      dataSet.printSchema()
    }

  }
}