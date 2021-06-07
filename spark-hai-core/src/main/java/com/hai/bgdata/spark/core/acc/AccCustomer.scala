package com.hai.bgdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 自定义 WordCount 累加器
 *
 * @author hai
 * @date 2021/6/4 17:36
 */
object AccCustomer {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(AccCustomer.getClass.getSimpleName).setMaster("local")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")

    val values: RDD[String] = context.makeRDD(List("hello spark", "hello java"))

    //task 分布式计算，Accumulator在executor端计算后会返回driver端
    //val sumAcc: LongAccumulator = context.longAccumulator("MyAccumulator")
    val myAccumulator = new MyWordCountAccumulator
    context.register(myAccumulator, "MyAccumulator")

    values.flatMap(s => s.split(" ")).foreach(s => myAccumulator.add(s))

    println(myAccumulator.value)
    context.stop()
  }

  /**
   * 自定义 WordCount 累加器
   */
  class MyWordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    val wcMap = mutable.Map[String, Long]()

    override def isZero: Boolean = {wcMap.isEmpty}

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {new MyWordCountAccumulator}

    override def reset(): Unit = {wcMap.clear()}

    override def add(word: String): Unit = {
      val newCount = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCount)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val curr = this.wcMap
      val update = other.value
      update.foreach {
        case (word, count) =>
          val newCount = curr.getOrElse(word, 0L) + count;
          curr.update(word, newCount)
      }
    }

    override def value: mutable.Map[String, Long] = {wcMap}
  }

}
