package Spark_Documentation

import scala.collection.mutable.ArrayBuffer

object Basic_function_code {
  def main(args: Array[String]): Unit = {
    var k:Array[Integer]=Array(1,2,3,4,5,6)

    var tt=sum(k)
    tt.foreach(l=>print(l))
  }

  def sum(a:Array[Integer]):ArrayBuffer[Integer]={
var oo:ArrayBuffer[Integer]=ArrayBuffer()
    for(i<-0 to a.size-1){
      oo+=a(a.size-1-i)

    }
oo
  }
}
