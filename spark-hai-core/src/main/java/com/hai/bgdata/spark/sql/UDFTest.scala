package com.hai.bgdata.spark.sql

/**
 * description
 *
 * @author hai
 * @date 2021/6/5 23:36
 */
object UDFTest extends BaseJsonSparkSql {

  def main(args: Array[String]): Unit = {
    init()
    udf()
    close()
  }

  def udf(): Unit = {
    sparkSession.udf.register("udfAddName", (v1: String) => "udfAddName: " + v1)
    sparkSession.udf.register("udfDouble", (v: Int) => v * 2)
    //    sparkSession.udf.register("udfDouble", (v: Any) => {
    //      var msg = ""
    //      var result = 0
    //      v match {
    //        case int: Int => {
    //          msg = "type:int"
    //          result = int * 2
    //        }
    //        case double: Double => {
    //          msg = "type:double"
    //          result = double * 2
    //        }
    //        case _ => {
    //          msg = "default"
    //          result = 0
    //        }
    //      }
    //      println("udfDouble: v=" + v + ", result=" + result)
    //      result
    //    })

    userDataFrame.createOrReplaceGlobalTempView("user")

    sparkSession.sql("select id, udfAddName(name), udfDouble(age) from global_temp.user").show()
  }
}
