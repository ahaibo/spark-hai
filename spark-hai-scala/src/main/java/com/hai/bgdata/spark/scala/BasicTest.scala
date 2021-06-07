package com.hai.bgdata.spark.scala

import java.util.Date

/**
 * description
 *
 * @author hai
 * @date 2021/6/5 12:15
 */
object BasicTest {

  def main(args: Array[String]): Unit = {
    basicTest()
    jjcfb()
    matchTest()
  }

  def basicTest(): Unit = {
    println("for (i <- 1 to 10)")
    for (i <- 1 to 10) {print(i + "\t")}

    println("\nfor (i <- 1 until 10)")
    for (i <- 1 until 10) {print(i + "\t")}

    println("\nfor (i <- 1 to 10; if (i % 2 == 0))")
    for (i <- 1 to 10; if (i % 2 == 0)) print(i + "\t")

    val result = for (i <- 1 to 100 if (i % 4 == 0)) yield i
    println("\nfor (i <- 1 to 100 if (i % 4 == 0)) yield i:\n" + result)
  }

  def jjcfb(a: Int = 9, b: Int = 9): Unit = {
    println("\n99乘法表:for (i <- 1 to 9; j <- 1 until 10)")
    for (i <- 1 to 9; j <- 1 until 10) {
      if (i >= j) {
        print(j + " * " + i + " = " + (i * j) + "\t")
      }
      if (i == j) {
        println()
      }
    }
  }

  def matchCase(value: Any): Unit = {
    var msg = "no match";
    value match {
      case 1 => msg = "1"
      case int: Int => msg = "int"
      case boolean: Boolean => msg = "boolean"
      case double: Double => msg = "double"
      case string: String => msg = "string"
      case char: Char => msg = "char"
      case list: List[Any] => msg = "List"
      case _ => msg = "other type"
    }
    println(value + " is " + msg)
  }

  def matchTest(): Unit = {
    val tuple: (Int, Boolean, Double, String, Nil.type, Char, Date) = (1, false, 2.2, "zhangsan", Nil, 'c', new Date())

    println("data: " + tuple)
    val iterator: Iterator[Any] = tuple.productIterator
    while (iterator.hasNext) {
      val value: Any = iterator.next()
      matchCase(value)
    }
  }

}
