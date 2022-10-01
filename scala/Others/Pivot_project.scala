package Others

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import shapeless.syntax.std.tuple.{productTupleOps, unitTupleOps}

import scala.collection.mutable.ArrayBuffer



object Pivot_project {
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




    val gang = dataSet.groupBy("InvoiceNo", "StockCode", "Country", "Quantity").pivot("Country").agg(count("Quantity")).toDF()

gang.show()
//    val deny = dataSet.select("Country")
//    val mm = deny.distinct()
//    val pp=mm.collect()
//
//    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer()
//var z=0
//
//    for(i<-0 to pp.size-1){
//      val k: String = pp(i).toString()
//      val m=k.subSequence(1,k.length-1).toString
////
////      if(m.indexOf(' ')==(-1)){
////
////      }
//
//
//      arrayBuffer1+= s"""\'$m\'"""
//      arrayBuffer1+= "`"+m+"`"
//
//z+=1
//    }
//
//val u=arrayBuffer1.toString().length
////print(arrayBuffer1.toString().subSequence(12,u))
//var k=arrayBuffer1.toString().subSequence(12,u)
//    k="("+z+","+k
//print(k)

   // gang.selectExpr("Company","stack(15,'11',11,'22',22,'33',33,'44',44,'55',55,'66',66,'77',77,'88',88,'99',99,'111',111,'222',222,'333',333,'444',444,'555',555,'7777',7777) as (Encoded_Country_Name,Quantity)").where ("Quantity is not null").show()
    //
   //gang.selectExpr("Company", s"stack($k) as (Country,Quantity)").where("Quantity is not null").show()

   // gang.selectExpr("Company", "stack(36,'Sweden', Sweden, 'Singapore', Singapore, 'Germany', Germany, 'RSA', RSA, 'France', France, 'Greece', Greece, 'Belgium', Belgium, 'Finland', Finland, 'Malta', Malta, 'Unspecified', Unspecified, 'Italy', Italy, 'EIRE', EIRE, 'Lithuania', Lithuania, 'Norway', Norway, 'Spain', Spain, 'Denmark', Denmark, 'Iceland', Iceland, 'Israel', Israel, 'Channel Islands', Channel Islands, 'USA', USA, 'Cyprus', Cyprus, 'Saudi Arabia', Saudi Arabia, 'Switzerland', Switzerland, 'United Arab Emirates', United Arab Emirates, 'Canada', Canada, 'Czech Republic', Czech Republic, 'Brazil', Brazil, 'Lebanon', Lebanon, 'Japan', Japan, 'Poland', Poland, 'Portugal', Portugal, 'Australia', Australia, 'Austria', Austria, 'Bahrain', Bahrain, 'United Kingdom', United Kingdom, 'Netherlands', Netherlands) as (Country,Quantity)").where("Quantity is not null").show()
    //
  // pp.foreach(f=>print(f))
}

}
