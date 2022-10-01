package Spark_Documentation

object Pattern_Matching {
  def main(args: Array[String]): Unit = {


    val giga = "jolly"

    var illi=giga match {
      case "jolly" => "YEs"
      case "Ram" => "NO"
    }
    print(illi)


  }
}
