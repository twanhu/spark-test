package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class SourceAnalysis {

  @Test
  def wordCount(): Unit = {
    // 1、创建sc对象
    val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("wordCount_source")
    val sc: SparkContext = new SparkContext(conf)

    // 2、创建数据集
    val textRDD: RDD[String] = sc.parallelize(Seq("hadoop spark", "hadoop flume", "spark sqoop"))

    // 3、数据处理
    //      1、拆词
    val splitRDD: RDD[String] = textRDD.flatMap(_.split(" "))
    //      2、赋予初始词频
    val tupleRDD: RDD[(String, Int)] = splitRDD.map((_, 1))
    //      3、聚合统计词频
    val reduceRDD: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    //      4、将结果转为字符串
    val strRDD: RDD[String] = reduceRDD.map(item => s"${item._1}, ${item._2}")

    // 4、结果获取
    strRDD.collect().foreach(println)

    // 5、关闭sc
    sc.stop()

  }

}
