package com.hai.bgdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description
 *
 * @author hai
 * @date 2021/6/4 17:36
 */
object AccTest3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(AccTest3.getClass.getSimpleName).setMaster("local")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")

    val values: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    //task 分布式计算，Accumulator在executor端计算后会返回driver端
    val sumAcc: LongAccumulator = context.longAccumulator("sumAcc3")
    val value: RDD[Unit] = values.map(i => {
      sumAcc.add(i)
    })
    //map算子不会触发实际计算
    println("sumAcc.map.value:" + sumAcc.value)

    //触发行动算子
    value.collect()
    println("sumAcc.map.collect1.value:" + sumAcc.value)

    //多次触发行动算子，计算多次
    value.collect()
    println("sumAcc.map.collect2.value:" + sumAcc.value)

    context.stop()
  }
}
