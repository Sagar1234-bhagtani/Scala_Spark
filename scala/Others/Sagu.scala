package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadMultipleCSVFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  println("spark read csv files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("C:/Users/sagar/Videos/Captures/cars.csv")

  //val df2 = spark.read.csv("/src/resources/file.csv")
  println(rddFromFile.getClass)

  val rdd = rddFromFile.map(f=>{
    f.split(";")
  })

  println("Iterate RDD")
  rdd.foreach(f=>{
    println("Col1:"+f(0))
  })
  println(rdd)

  println("Get data Using collect")
  rdd.collect().foreach(f=>{
    println("Col1:"+f(0))
  })

//val count=r
  val dfFromRDD1 = rdd.toDF("Bull")
  dfFromRDD1.printSchema()



  dfFromRDD1.show()
//  println("read all csv files from a directory to single RDD")
//  val rdd2 = spark.sparkContext.textFile("C:/tmp/files/*")
//  rdd2.foreach(f=>{
//    println(f)
//  })
//
//  println("read csv files base on wildcard character")
//  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text*.csv")
//  rdd3.foreach(f=>{
//    println(f)
//  })
//
//  println("read multiple csv files into a RDD")
//  val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.csv,C:/tmp/files/text02.csv")
//  rdd4.foreach(f=>{
//    println(f)
//  })

}