
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

object taskk_testing {

  var ss=0

  var sd =0
  var si =0
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkByExamples.com")
      .getOrCreate()
val t1=System.nanoTime()

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
      //.option("inferSchema", "false")

      .load("C:/Users/sagar/Videos/Captures/part1.xls")

dataSet.show()
    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer()

    var h=dataSet.count()
    print(h)

    var m:Int=0
    dataSet.dtypes.foreach(f=>arrayBuffer1+=f._1.toString())
    arrayBuffer1.foreach(r=>print(r))
    dataSet.printSchema()

    for (i <- 0 to arrayBuffer1.size - 2) {


      val df9 = dataSet.select(s"${arrayBuffer1(i).toString()}")
      ss = 0
      sd = 0
      si = 0




      df9.foreach { row =>

        m=m+1
        row.toSeq.foreach { col =>




          try{
            //IMPORTANT AREA



            try {
              si=1
              var t=col.toString
              t.toInt
              print(t+" => Integer")




            } catch {
              case e: Exception => print("")

                try {
                  var t = col.toString
                  t.toDouble
                  print(arrayBuffer1(i))

                  println(t+" => Double")
                  sd=1
                }

                catch {
                  case e: Exception => println(col.toString+" => String")
                    ss=1

                }
            }
            //IMPORTANT AREA END
          }
          catch {
            case e: Exception => print("")
          }
        }

      }
      println()
      println("outside_______________")
      print(ss+""+sd+""+si+"**")
      if (ss == 0  && sd == 0 && si == 1) {
        print("There are this number of rows and column is 100% Integer" + m + " " + si)

        val gh = dataSet.withColumn(s"${arrayBuffer1(i)}", col(s"${arrayBuffer1(i)}").cast("Integer"))
        gh.printSchema()
        dataSet=gh

      }
      if (ss == 0 && sd == 1 &&si==1) {
        print("There are this number of rows and column is 100% Double" + m + " " + sd)
        val gh = dataSet.withColumn(s"${arrayBuffer1(i)}", col(s"${arrayBuffer1(i)}").cast("double"))
        gh.printSchema()
        dataSet=gh
        //gh.show()
      }
      if (m == h && ss == 1) {
        print("There are this number of rows and column is 100% STring" + m + " " + ss)
      }


      //print(m+"**")

    }
    dataSet.printSchema()
    dataSet.show()

    print((System.nanoTime()-t1)/1000000+"*******")
  }

}