package com.hai.bgdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description
 *
 * @author hai
 * @date 2021/6/5 23:02
 */
trait BaseSpark {

  var filePath: String = null
  var sparkConf: SparkConf = null
  var sparkContext: SparkContext = null

  def init(): Unit = {
    printSplitLine("init")
    sparkConf = new SparkConf().setMaster("local").setAppName("DataFrameSetTest")
    sparkContext = new SparkContext(sparkConf)
    printClassInitLog("BaseSpark")
  }

  def close(): Unit = {
    printSplitLine("close")
    if (null != sparkContext) {
      sparkContext.stop()
    }
  }

  def printSplitLine(title: String, s: String = "#", size: Int = 50): Unit = {
    val builder = new StringBuilder(60)
    for (i <- 1 to size) builder.append(s)
    println("\n\n" + builder.append(" ").append(title))
  }

  /*def printClassLog[T: Class](clazz: Class[T], content: String = ""): Unit = {
    printClassLog(clazz, "", content, "")
  }

  def printClassLog[T: Class](clazz: Class[T], content: String = "", suffix: String = ""): Unit = {
    printClassLog(clazz, "", content, suffix)
  }*/

  def printClassInitLog(path: String = ""): Unit = {
    println(path + " init...")
  }
}
