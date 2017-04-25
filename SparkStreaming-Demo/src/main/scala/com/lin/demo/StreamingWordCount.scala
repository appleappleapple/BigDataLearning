package com.lin.demo
import org.apache.spark.SparkConf
import org.apache.spark.streaming._


object StreamingWordCount {


  def main(args: Array[String]) {
    //开本地线程两个处理
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    //每隔1秒计算一批数据
    val ssc = new StreamingContext(conf, Seconds(1))
    //监控机器ip为192.168.1.187:9999端号的数据,注意必须是这个9999端号服务先启动nc -l 9999，否则会报错,但进程不会中断
    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    //按空格切分输入数据
    val words = lines.flatMap(_.split(" "))
    //计算wordcount
    val pairs = words.map(word => (word, 1))
    //word ++
    val wordCounts = pairs.reduceByKey(_ + _)
    //排序结果集打印，先转成rdd，然后排序true升序，false降序，可以指定key和value排序_._1是key，_._2是value
    val sortResult=wordCounts.transform(rdd=>rdd.sortBy(_._2,false))
    sortResult.print()
    ssc.start()             // 开启计算
    ssc.awaitTermination()  // 阻塞等待计算

  }


}