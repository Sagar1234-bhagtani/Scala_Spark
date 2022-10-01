package Spark_Documentation

object Fibbo_series {
  def main(args:Array[String]): Unit ={
   // fibbo(0,1):Array[Int] for
  }

  def fibbo(a:Int,b:Int): Unit ={
    var aa=a
    var bb=b
    for(i<-1 to 100){
     var c=aa+bb
      print(c+" ")
      aa=bb
      bb=c


    }
  }


}
