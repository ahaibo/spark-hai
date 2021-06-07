package com.hai.bgdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description
 *
 * @author hai
 * @date 2021/6/3 17:25
 */
object WordCount1 {

  def main(args: Array[String]): Unit = {
    println("hello spark")

    //application
    //spark 框架
    //配置信息
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount1")
    //建立连接
    val sc = new SparkContext(sparkConf)

    //执行业务
    val lines = sc.textFile("data/input/*.txt")

    //    val words = lines.flatMap(s => s.split(""))
    val words = lines.flatMap(_.split(" "))

    val wordGroup = words.groupBy(word => word)

    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val tuples = wordToCount.collect().foreach(println)

    //关闭连接
    sc.stop()
  }
}
