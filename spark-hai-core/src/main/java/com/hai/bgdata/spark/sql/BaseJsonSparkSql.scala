package com.hai.bgdata.spark.sql

/**
 * description
 *
 * @author hai
 * @date 2021/6/5 23:02
 */
trait BaseJsonSparkSql extends BaseSparkSql {

  override def init(): Unit = {
    super.init()
    filePath = "data/input/json/user.json"
    userDataFrame = sparkSession.read.json(filePath)
    printClassInitLog("BaseJsonSparkSql")
  }

}
