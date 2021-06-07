package com.hai.bgdata.spark.sql

import com.hai.bgdata.spark.model.User
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * description
 *
 * @author hai
 * @date 2021/6/5 19:58
 */
object DataFrameSetTest extends BaseJsonSparkSql {

  override def init() {
    super.init()
    printClassInitLog("DataFrameSetTest")
  }

  def main(args: Array[String]): Unit = {
    init()
    sql()
    //dataFrame()
    //toJSON()
    toDataSet()
    close()
  }


  def sql(): Unit = {
    printSplitLine("sql")
    //查全局视图要加 global_temp. 前缀
    userDataFrame.createOrReplaceGlobalTempView("user")
    println("sparkSession.sql(\"select name,age from global_temp.user\")")
    sparkSession.sql("select name,age from global_temp.user").show()

    println("sparkSession.newSession().sql(\"select name,age from global_temp.user where age>30\")")
    sparkSession.newSession().sql("select name,age from global_temp.user where age>30").show()
  }

  def dataFrame(): Unit = {
    printSplitLine("dataFrame")
    println("userDataFrame.show()")
    userDataFrame.show()

    println("userDataFrame.show(2)")
    userDataFrame.show(2)

    println("userDataFrame.show(true)")
    userDataFrame.show(true)

    println("userDataFrame.select(\"id\", \"name\", \"age\").show()")
    userDataFrame.select("id", "name", "age").show()

    println("userDataFrame.selectExpr(\"age<30\").show()")
    userDataFrame.selectExpr("age<30").show()
  }

  def toJSON(): Unit = {
    printSplitLine("toJSON")
    val userDS: Dataset[String] = userDataFrame.toJSON

    val userDS2: Dataset[Row] = userDataFrame.as("abc")
    println("userDS2.printSchema()")
    userDS2.printSchema()

    println("userDS2.select(\"id\", \"name\", \"age\").where(\"age < 30\").show()")
    userDS2.select("id", "name", "age").where("age < 30").show()

    println("userDS.printSchema()")
    userDS.printSchema()

    println("userDS.select(\"*\").show()")
    userDS.select("*").show()


    val rdd: RDD[Row] = userDataFrame.rdd
    //    rdd.foreachAsync(row => {
    rdd.foreach(row => {
      val name: String = row.getAs("name")
      val age: Any = row.getAs("age")
      println(name + " : " + age)
    })

    //TimeUnit.HOURS.sleep(1)
  }

  def toDataSet(): Unit = {
    printSplitLine("toDataSet")
    val sparkSession2 = SparkSession.builder().config(sparkConf).getOrCreate()
    val frame: DataFrame = sparkSession2.read.json(filePath)
    import sparkSession2.implicits._

    //frame.as 据 model 转 DataSet 时必须导入 import sparkSession2.implicits._
    val value: Dataset[User] = frame.as[User]
    value.printSchema()

    val relationalGroupedDataset: RelationalGroupedDataset = value.groupBy("age")
    println("value.groupBy(\"age\").count().orderBy(new Column(\"count\").desc).limit(5).show(3)")
    relationalGroupedDataset.count().orderBy(new Column("count").desc).limit(5).show(3)
  }

}
