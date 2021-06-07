package com.hai.bgdata.spark.core.rdd

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计各省排名前三的广告
 *
 * @author hai
 * @date 2021/6/4 12:12
 */
object AdTopStat {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(AdTopStat.getClass.getSimpleName).setMaster("local")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")

    //checkpoint dir
    //context.setCheckpointDir("cp")

    //时间戳，省份，城市，用户，广告，中间字段使用空格分隔
    val lines: RDD[String] = context.textFile("data/input/ad-data.txt")

    //((省，广告)，1)
    val provinceGroup: RDD[((String, String), Int)] = lines.map(line => {
      val arr: Array[String] = line.split(" ")
      ((arr(1), arr(4)), 1)
    })

    //((省，广告)，总和)
    val provinceSumGroup: RDD[((String, String), Int)] = provinceGroup.reduceByKey(_ + _)

    //落盘，任务执行完后不会删除
    //provinceSumGroup.checkpoint();

    //缓存到内存 StorageLevel.MEMORY_ONLY
    //provinceSumGroup.cache()
    //provinceSumGroup.persist()

    //落盘，临时存储，任务完成后删除
    //provinceSumGroup.persist(StorageLevel.DISK_ONLY)

    //(省，(广告，总和))
    val provinceAdSumGroup: RDD[(String, (String, Int))] = provinceSumGroup.map {
      case ((province, ad), sum) => (province, (ad, sum))
    }

    //(省，((广告，总和),...))
    val provinceAdTop: RDD[(String, Iterable[(String, Int)])] =
      provinceAdSumGroup.groupByKey()
        .mapValues(iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))

    provinceAdTop.collect().foreach(println)

    TimeUnit.HOURS.sleep(1)

    context.stop()
  }
}
