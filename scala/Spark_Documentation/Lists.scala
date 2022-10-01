package Spark_Documentation

import scala.collection.mutable.ArrayBuffer

object Lists {
  def main(args: Array[String]): Unit = {

    //___________________________________________LIST________________________________
    var lst: List[String]=List("sagar","Rohan","Raj Sir")
var p=0
    lst.foreach(g=>
    if(g.equals("sagar")){
      p=1
      print("yes present")


    }


    )
    if(p==0)
      print("Not present")
println()
    //__________________________________MAP__________________________________________

    var map:Map[String,Integer]=Map("Sagar"->23,"Rohan"->45)

   print(map("Sagar"))

//_____________________________________ARRAYBUFFER___________________________________________________

    println()


var arraybuffer:ArrayBuffer[String]=ArrayBuffer("Hola","hi","hey")

    arraybuffer.foreach(h=>println(h))

  }
}
