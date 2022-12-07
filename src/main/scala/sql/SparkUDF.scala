package sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object SparkUDF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("world")
      .master("local[3]")
      .getOrCreate()

    val data: DataFrame = spark.read.csv("dataset/demodata/stuInfo.csv").toDF("id", "name", "age")


    data.createOrReplaceTempView("stuInfo")

    // 自定义函数,register() : 注册为用户定义函数
    spark.udf.register("myavg", functions.udaf(new MyAvgUDAF))

    spark.sql("select myavg(age) from stuInfo").show()

    spark.close()
  }

  /**
   * 自定义聚合函数类：计算平均值
   * 继承 Aggregator 定义泛型
   * IN : 输入的数据类型
   * BUF : 缓冲区的数据类型
   * OUT : 输出的数据类型
   */
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    // z & zero : 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区(spark存在分区)
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    // 计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    // 缓冲区的编码操作,自定义的类用 Encoders.product
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}



























