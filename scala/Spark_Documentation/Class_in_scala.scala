package Spark_Documentation

class User(var name:String,var age:Int);

object Class_in_scala {
  def main(args: Array[String]): Unit = {
    var user=new User("Sagar",23)
    println(user.name)
    println(user.age)

    user.name="Gaytri"
    user.age=100

    println(user.name)
    println(user.age)


  }

}
