package Spark_Documentation

import scala.collection.mutable

object Queue_in_scala {

  def main(args: Array[String]): Unit = {

    var rr:mutable.Queue[String]=mutable.Queue("sagar","Roni")
rr.foreach(t=>print(t))
  }


}
