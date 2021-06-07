package com.hai.bgdata.spark.sql

import com.hai.bgdata.spark.BaseSpark
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * description
 *
 * @author hai
 * @date 2021/6/5 23:02
 */
trait BaseSparkSql extends BaseSpark {
  var sparkSession: SparkSession = null
  var userDataFrame: DataFrame = null

  override def init(): Unit = {
    super.init()
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    printClassInitLog("BaseSparkSql")
  }

  override def close() {
    if (null != sparkSession) {
      sparkSession.close()
    }
  }
}
