package com.lin.wordcount

  import org.apache.spark.SparkConf
 import org.apache.spark.SparkContext
 import org.apache.spark.SparkContext._

 /**
  * 在安装有Spark的Linux机器上跑
  * 环境：CentOS
  * scala版本：2.11.8
  * Spark版本：1.6.1
  * hadoop版本：hadoop2.6.4
  */
object WordCount {
   def main(args: Array[String]) {
    if (args.length < 1) {
       System.err.println("Usage: <file>")
       System.exit(1)
     }
 
     val conf = new SparkConf()
     val sc = new SparkContext(conf)
     val line = sc.textFile(args(0))
 
     line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)
     
     val result = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.collect().foreach(println)
    result.saveAsSequenceFile(args(1)) 
 
     sc.stop()
   }
}