package TimeStamp

import org.apache.avro.LogicalTypes.date
import org.apache.commons.collections.CollectionUtils.select
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.date_format

object Try_timeStamp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    //print(k)
//   var k= spark.sql("select time_format(date '11-16-2019 16 44 55 406', \"H\")").collect()
//print(k(0))

//select.convert
//    spark.sql("SELECT CONVERT(varchar, '2017-08-25', 101)")

    spark.sql("select convert(varchar, getdate(), 2)")
  }
}
