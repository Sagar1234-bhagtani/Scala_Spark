package Spark_Documentation

object List_in_java {
  def main(args: Array[String]): Unit = {

    val mylist:List[Int]=List(1,2,3,4,5,6,7,8)

    print(mylist)
    println(0::mylist)
    print(mylist.tail)
    println(mylist.head)
    print(mylist.reverse)
    print(List.fill(5)(2)); //Create new list

    mylist.foreach(println)

    var sum=0;
    mylist.foreach(sum+=_);
    println(sum)

    for(name<-mylist){
      print(name)
    }
  }
}
