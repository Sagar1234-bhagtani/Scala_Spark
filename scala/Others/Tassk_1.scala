package Others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

import scala.collection.mutable.ArrayBuffer

object Tassk_1 {
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
      .load("C:/Users/sagar/Videos/Captures/Motu.xlsx")
    //
    //dataSet.printSchema()
    //    dataSet.show()
    dataSet.createOrReplaceTempView("jaadu")


    val q = "12,'Result1',Result1,'Result2',`Result2`,'Result3',Result3,'Resu lt4',`Resu lt4`,'Result5',Result5,'Result6',Result6,'Result7',Result7,'Result8',Result8,'Result9',Result9,'Result10',Result10,'Resutt11',Resutt11,'Result12',Result12"
    val df5 = spark.sql(s"Select Name, stack($q) as (Result,Attandance) from jaadu")
    // Jo braces me hai unka name chage kr skte ho
    df5.show(false)
df5.createOrReplaceTempView("bhaag")

    val deny = df5.select("Result")
    val mm = deny.distinct()
    val pp = mm.collect()

    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer("detnjn","ewfbub","wefuige")
    print(arrayBuffer1(0))
    var z = 0

    for (i <- 0 to pp.size - 1) {
     val k: String = pp(i).toString()
      val m = k.subSequence(1, k.length - 1).toString


      //print(m+" ")
      //
      //      if(m.indexOf(' ')==(-1)){
      //
      //      }


     arrayBuffer1 += s"""\'$m\'"""
     arrayBuffer1 += "`" + m + "`"

      z += 1


    }


    val u = arrayBuffer1.toString().length
    //print(arrayBuffer1.toString().subSequence(12,u))
    var k = arrayBuffer1.toString().subSequence(12, u).toString
    k = "(" + z + "," + k
    print(k)

   val gang=df5.groupBy("Name").pivot("Result").agg(count("Attandance") ).toDF()

    df5.printSchema()


    //val gang=df5.groupBy("Name").pivot("Result").sum("Attandance").toDF()

//gang.show()
    //val df9 = spark.sql(s"Select Name, stack($k) as (Country,Amount) from bhaag")
   // df9.show()
  // print(k)
  }
}
