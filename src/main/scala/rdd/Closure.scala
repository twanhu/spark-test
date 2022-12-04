package rdd

import org.junit.Test

class Closure {

  /**
   * 编写一个高阶函数，在这个函数内要有一个变量，返回一个函数，通过这个变量完成一个计算
   */
  @Test
  def test(): Unit = {
    val f: Int => Double = closure()
    val area = f(5)
    println(area)

  }

  /**
   * 返回一个新的函数
   */
  def closure(): Int => Double = {
    val factor = 3.14
    val areaFunction = (r: Int) => math.pow(r, 2) * factor
    areaFunction
  }

}
