package com.hai.bgdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description
 *
 * @author hai
 * @date 2021/6/4 17:36
 */
object AccTest1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(AccTest1.getClass.getSimpleName).setMaster("local")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")

    val values: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    var sum = 0
    values.foreach(i => {
      sum += i
    })

    //task 分布式计算，sum在executor端计算后不会返回driver端
    println("sum:" + sum)

    context.stop()
  }
}
