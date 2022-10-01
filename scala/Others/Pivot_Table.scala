import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, when}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import shapeless.syntax.std.tuple.unitTupleOps
object giga {
      def main(args:Array[String]): Unit ={

            val spark:SparkSession = SparkSession.builder()
              .master("local[5]")
              .appName("SparkByExamples.com")
              .getOrCreate()


            val dataSet =spark.read.format("com.crealytics.spark.excel")
              .option("header","true")
              //.option("dataAddress","Sheet1")
              .option("maxRowsInMemory", 500)

              .option("InferSchama","True")
              .load("C:/Users/sagar/Videos/Captures/OnlineRetail.xlsx")

            val deny=dataSet.select("Country")
            val mm=deny.distinct()
            mm.show()
            dataSet.printSchema()
            val df3 = dataSet.withColumn("Encoded_Country_Name", when(col("Country") === "United Kingdom","11")
              .when(col("Country") === "France","22").when(col("Country") === "Australia","33")
              .when(col("Country") === "Netherlands","44").when(col("Country") === "Germany","55")
              .when(col("Country") === "Norway","66").when(col("Country") === "EIRE","77")
              .when(col("Country") === "Spain","88").when(col("Country") === "Poland","99")
              .when(col("Country") === "Italy","111").when(col("Country") === "Belgium","222")
              .when(col("Country") === "Lithuania","333").when(col("Country") === "USA","444")
              .when(col("Country") === "Sweden","555").otherwise("7777"))


           // val gang=df3.groupBy("InvoiceNo", "StockCode","Encoded_Country_Name","Quantity").pivot("Encoded_Country_Name").agg(count("Quantity")).toDF()

            val gang=df3.groupBy("InvoiceNo", "StockCode","Encoded_Country_Name","Quantity").pivot("Encoded_Country_Name").agg(count("Quantity")).toDF()
            gang.show()
            gang.createOrReplaceTempView("jain")
            //spark.sql("select count(Encoded_Country_Name) from jain group by InvoiceNo").show//gang.show()
            //print(gang.count())
            // unpivot

            //
            //    val unPivotDF = gang.select($"Product",expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) ")).where("Total is not null")
            //    //unPivotDF.show()
            //
            //    val hh=gang.groupBy("fewef",expr("length(word)")).count()

            //  spark.sql("select product, stack({0},{1} as (Country,Amount) from PivotTable")
            //gang.selectExpr("Company","stack(15,'11',11,'22',22,'33',33,'44',44,'55',55,'66',66,'77',77,'88',88,'99',99,'111',111,'222',222,'333',333,'444',444,'555',555,'7777',7777) as (Encoded_Country_Name,Quantity)").where ("Quantity is not null").show()
            val df5=spark.sql("Select InvoiceNo,StockCode,Quantity, stack(15,'11',11,'22',22,'33',33,'44',44,'55',55,'66',66,'77',77,'88',88,'99',99,'111',111,'222',222,'333',333,'444',444,'555',555,'7777',7777) as (Country,Amount) from jain")

            val df6=df5.drop("Amount")
            df6.show()
            print(df6.count())
            //val pp=
            //val df4=df3.drop("Country")
            //    df4.show(10000)
      }
}