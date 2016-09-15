package com.lin.pagerank

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object PageRankLocal {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("PageRankLocal")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val links = sc.parallelize(List(("A", List("B", "C")), ("B", List("A", "C")), ("C", List("A", "B", "D")), ("D", List("C")))).partitionBy(new HashPartitioner(100)).persist()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }

    ranks.sortByKey().collect()
  }
}