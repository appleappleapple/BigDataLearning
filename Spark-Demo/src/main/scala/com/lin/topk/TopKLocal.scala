package com.lin.topk

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 本地运行Spark
 * 环境：Windows7
 * scala版本：2.11.8
 * Spark版本：1.6.1
 */
object TopKLocal {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("TopKLocal")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D://Java//spark//spark-1.6.1-bin-hadoop2.6//README.md", 1)
    val result = lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
    val sorted = result.map { case (key, value) => (value, key) }.sortByKey(true, 1)
    val topk = sorted.top(10) //10自己定义
    topk.foreach(println)
    sc.stop
  }

}