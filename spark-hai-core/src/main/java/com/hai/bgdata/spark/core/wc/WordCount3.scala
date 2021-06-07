package com.hai.bgdata.spark.core.wc

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description
 *
 * @author hai
 * @date 2021/6/3 17:25
 */
object WordCount3 {

  def main(args: Array[String]): Unit = {
    //println("hello spark")

    //application
    //spark 框架
    //配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount23")
    //建立连接
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //执行业务
    val lines = sc.textFile("data/input/*.txt")

    //    val words = lines.flatMap(s => s.split(""))
    val words = lines.flatMap(_.split(" "))

    //val wordToOne = words.map(word => (word, 1))
    val wordToOne = words.map((_, 1))

    //val wordToCount = wordToOne.reduceByKey((x,y)=>{x+y})
    val wordToCount = wordToOne.reduceByKey(_ + _)

    //wordToCount.foreach(println)
    val tuples = wordToCount.foreach(println)

    println(wordToCount.collect().toBuffer)

    //output
    wordToCount.repartition(1).saveAsObjectFile("data/output/wc3")

    //观察spark-ui
    TimeUnit.HOURS.sleep(1);

    //关闭连接
    //sc.stop()
  }
}
