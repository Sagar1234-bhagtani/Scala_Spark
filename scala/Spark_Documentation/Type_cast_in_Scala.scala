package Spark_Documentation

object Type_cast_in_Scala {

  def main(args: Array[String]): Unit = {

    var p:Integer=67
    println(p.getClass)//
    println((p.toDouble).getClass)
    print(p.toString.getClass)
  }

}
