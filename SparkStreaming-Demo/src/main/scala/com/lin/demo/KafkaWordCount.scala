package com.lin.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Durations
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]") //至少2个线程，一个DRecive接受监听端口数据，一个计算
    val sc = new StreamingContext(sparkConf, Durations.seconds(3));
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "127.0.0.1:9092") // 然后创建一个set,里面放入你要读取的Topic,这个就是我们所说的,它给你做的很好,可以并行读取多个topic
    var topics = Set[String]("linlin");
    //kafka返回的数据时key/value形式，后面只要对value进行分割就ok了
    val linerdd = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, topics)
    val wordrdd = linerdd.flatMap { _._2.split(" ") }
    wordrdd.foreachRDD(rdd => {
      println("从topic:" + topics + "读取rdd:" + rdd.count())
    })

    wordrdd.print()
    val resultrdd = wordrdd.map { x => (x, 1) }.reduceByKey { _ + _ }
    resultrdd.print()
    sc.start()
    sc.awaitTermination()
    sc.stop()
  }

}