package com.lin.flink.demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * 可以直接本地运行
 */
object WordCount {

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.createLocalEnvironment(1)

    //从本地读取文件
    val text = env.readTextFile("D:/Java/flink-1.2.0-bin-hadoop27-scala_2.11/flink-1.2.0-bin-hadoop27-scala_2.11/flink-1.2.0/README.txt")

    //单词统计
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    //输出结果
    counts.print()

    //保存结果到txt文件
    counts.writeAsText("D:/output.txt", WriteMode.OVERWRITE)
    env.execute("Scala WordCount Example")

  }
}