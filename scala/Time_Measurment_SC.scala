object Time_Measurment_SC {

  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
var sum:Long=0
    for(i<-0 to 2){

      sum=sum+i
    }
    println((System.nanoTime-t1)/1000000)
println(sum)
  }
}
