package Spark_Documentation

object Bubble_Sort {
def main(args:Array[String]): Unit ={
  bubble_Sort(Array(50,33,44,55,100)) foreach println
}

  def bubble_Sort(a:Array[Int]):Array[Int]={
    for(i<-1 to a.length-1){
      for(j<-(i-1) to 0 by -1){

        if(a(j)>a(j+1)){
          val temp=a(j+1)
          a(j+1)=a(j)
          a(j)=temp
        }
      }
    }
    a
  }

}
