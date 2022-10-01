package Others

import org.apache.spark.sql.SparkSession

import java.io.{FileNotFoundException, IOException}
import scala.collection.mutable.ArrayBuffer

object Exxceptions_in_scala {
  def main(args: Array[String]): Unit = {
   // val a = scala.io.StdIn.readLine()

//
//    val spark: SparkSession = SparkSession.builder()
//      .master("local[1]")
//      .appName("SparkByExamples.com")
//      .getOrCreate()
//
//
//    val dataSet = spark.read.format("com.crealytics.spark.excel")
//      .option("header", "true")
//      //.option("dataAddress","Sheet1")
//      .option("maxRowsInMemory", 500)
//
//      .option("InferSchema", "True")
//      .load("C:/Users/sagar/Videos/Captures/Book2.xlsx")





   // val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer("4.5a", "3.3", "5","ewnngr","45")
    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer("sfef", "grgjn", "wefjbhfwb")

    //val coco=("Sagar",55,23,7.6,"w3mtw")
   // println("The value of a is " + a)
    //var text = ""e
    //print(coco._2)

for(i<-0 to arrayBuffer1.size-1) {

  try {

    //      val a = scala.io.StdIn.readLine()
    //      println("The value of a is " + a)

    arrayBuffer1(i).toInt
    println("Integer")

  } catch {
    case e: Exception => print("")

      try {
        //          val a = scala.io.StdIn.readLine()
        //          println("The value of a is " + a)
        arrayBuffer1(i).toDouble
        //a.to
        println("Double")
      }

      catch {
        case e: Exception => println("String")

      }

  }
}
}
}
