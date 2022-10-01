package Others



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

object Exception_Progess {

  var ss=0
  var sd =0
  var si =0

  def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .master("local[1]")
          .appName("SparkByExamples.com")
          .getOrCreate()


//        var dataSet = spark.read.format("com.crealytics.spark.excel")
//          .option("header", "true")
//          //.option("dataAddress","Sheet1")
//          .option("maxRowsInMemory", 500)
//          //.option("InferSchema", "True")
//          .load("C:/Users/sagar/Videos/Captures/Book2.xlsx")

       var dataSet = spark.read
            .format("com.crealytics.spark.excel")

            .option("header", "true")
            .load("C:/Users/sagar/Videos/Captures/part1.xls")




var h=dataSet.count()
    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer()



    var m:Int=0
    dataSet.dtypes.foreach(f=>arrayBuffer1+=f._1)
    arrayBuffer1.foreach(r=>print(r))
    dataSet.printSchema()

    for (i <- 0 to arrayBuffer1.size - 1) {


      val df9 = dataSet.select(s"${arrayBuffer1(i)}")
      ss = 0
      sd = 0
      si = 0
      df9.foreach { row =>
        m=m+1
        row.toSeq.foreach { col =>

          try{

            try {
              si = 1
              var t = col.toString
              t.toInt
            }
             catch {
              case e: Exception => print("")

                try {
                  var t = col.toString
                  t.toDouble
                  sd=1
                }

                catch {
                  case e: Exception => print(col.toString)
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


     if (ss == 0  && sd == 0 && si == 1) {
             val gh = dataSet.withColumn(s"${arrayBuffer1(i)}", col(s"${arrayBuffer1(i)}").cast("integer"))
              gh.printSchema()
               dataSet=gh
     }

      if (ss == 0 && sd == 1 &&si==1) {

        val gh = dataSet.withColumn(s"${arrayBuffer1(i)}", col(s"${arrayBuffer1(i)}").cast("double"))
        gh.printSchema()
        dataSet=gh
        dataSet.show()

      }



      //print(m+"**")

    }
    dataSet.printSchema()
    dataSet.show()
  }

}
