package com.hai.bgdata.spark.scala

/**
 * description
 *
 * @author hai
 * @date 2021/6/5 12:16
 */
object FunctionTest {

  def main(args: Array[String]): Unit = {
    println(fun(10, 20))
    println(fun(10, 20)("hello", "world"))

    println(fun2(10, 20, (a, b) => {
      val result = a * b
      println("fun2.参数函数执行：" + a + " * " + b + " = " + result)
      result
    })("hello2", "world2"))

    println(kelihua(1, 2)(3)(4, "result-"))
  }

  /**
   * 返回值是函数
   *
   * @param a
   * @param b
   * @return
   */
  def fun(a: Int, b: Int): (String, String) => String = {
    val result = a * b;
    //嵌套函数
    def fun1(s1: String, s2: String): String = {
      s1 + "@" + s2 + "#" + result
    }

    fun1
  }

  /**
   * 参数，返回值都是函数
   *
   * @param a
   * @param b
   * @param f
   * @return
   */
  def fun2(a: Int, b: Int, f: (Int, Int) => Int): (String, String) => String = {
    println("fun2 调用参数函数。。。")
    val result = f(a, b)

    def fun3(s1: String, s2: String): String = {
      val fun3Result = s1 + "@@" + s2 + "##" + result
      println("fun2.fun3.result: " + fun3Result)
      fun3Result
    }
    //返回函数
    fun3
  }

  /**
   * 柯里化函数
   * 高阶函数的简化版
   *
   * @param a
   * @param b
   * @param c
   * @param d
   * @param e
   * @return
   */
  def kelihua(a: Int, b: Int)(c: Int)(d: Int, e: String): String = {
    "柯里化函数调用：" + e + (a + b + c + d)
  }
}
