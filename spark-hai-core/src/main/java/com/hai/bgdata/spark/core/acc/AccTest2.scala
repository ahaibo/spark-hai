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
object AccTest2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(AccTest2.getClass.getSimpleName).setMaster("local")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")

    val values: RDD[Int] = context.makeRDD(List(1, 2, 3, 4))

    //task 分布式计算，Accumulator在executor端计算后会返回driver端
    val sumAcc: LongAccumulator = context.longAccumulator("sumAcc2")
    values.foreach(i => {
      sumAcc.add(i)
    })
    println("sumAcc.foreach.value:" + sumAcc.value)

    context.stop()
  }
}
