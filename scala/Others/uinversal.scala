package Others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object uinversal {



    def main(args:Array[String]): Unit ={


      val spark:SparkSession = SparkSession.builder()
        .master("local[5]")
        .appName("SparkByExamples.com")
        .getOrCreate()

      val dataSet =spark.read.format("com.crealytics.spark.excel")
        .option("header","true")
        //.option("dataAddress","Sheet1")
        .option("maxRowsInMemory", 500)
        //.option("InferSchema","True")
        .load("C:/Users/sagar/Videos/Captures/OnlineSales.xlsx")

      dataSet.printSchema()


      print("Enter type in which you want to convert Quantity")
      val p = scala.io.StdIn.readLine()

      print("Enter type in which you want to convert Sales")
      val q = scala.io.StdIn.readLine()

      print("Enter type in which you want to convert Discount")
      val s = scala.io.StdIn.readLine()
      print("Enter column name ")
      val m = scala.io.StdIn.readLine()

      //    print("Enter type in which you want to convert Ship Mode")
      //    val s = scala.io.StdIn.readLine()
      //
      //    print("Enter type in which you want to convert Customer ID")
      //    val t = scala.io.StdIn.readLine()
      //
      //    print("Enter type in which you want to convert City")
      //    val u = scala.io.StdIn.readLine()
      //
      //    print("Enter type in which you want to convert City")
      //    val v = scala.io.StdIn.readLine()







      //_______________________________________
      //var gh=dataSet.withColumn(("Quantity",col("Quantity").cast(s"$p")),("Sales",col("Sales").cast(s"$q")),("Discount",col("Discount").cast(s"$r")))
      for(i<-0 to 5) {


        try {
          val gh = dataSet.withColumn(s"$m", col(s"$m").cast(s"$q")) //Second one is more Important
        }
        catch {
          case e: Exception => print("")
        }
      }
      val gh = dataSet.withColumn(s"$m", col(s"$m").cast(s"$q"))
      val ph=gh.withColumn("Sales",col("Sales").cast(s"$q"))
      //
      val mh=ph.withColumn("Discount",col("Discount").cast(s"$s"))
      //
      //    var gh=dataSet.withColumn("Quantity",col("Quantity").cast(s"$t"))

      mh.printSchema()
      mh.show()




    }


}
