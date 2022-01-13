package com.wxit.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 建立和Spark框架的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务操作

    val lines: RDD[String] = sc.textFile("data")


    val words: RDD[String] = lines.flatMap((_: String).split(' '))
    val wordToOne: RDD[(String, Int)] = words.map(
      (word: String) => (word, 1)
    )


    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      (t: (String, Int)) => t._1
    )


    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        val wordCount: (String, Int) = list.reduce(
          (t1: (String, Int), t2: (String, Int)) => {
            (t1._1, t1._2 + t2._2)
          }
        )
        wordCount
      }
    }


    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    //TODO 关闭连接
    sc.stop()
  }

}
