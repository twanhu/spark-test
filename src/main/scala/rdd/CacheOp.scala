package rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.junit.Test

class CacheOp {

  val conf: SparkConf = new SparkConf().setAppName("cache").setMaster("local[3]")
  val sc: SparkContext = new SparkContext(conf)

  @Test
  def prepare(): Unit = {

    // 1、读取文件
    val source: RDD[String] = sc.textFile("dataset/hoteldata/jiudian.csv")

    // 2、取出省份，赋予初始频率
    val countRDD: RDD[(String, Int)] = source.map(item => (item.split(",")(3), 1))

    // 3、数据清洗
    val article1: (String, Int) = countRDD.first()
    val cleanRDD: RDD[(String, Int)] = countRDD.filter(item => StringUtils.isNotEmpty(item._1) && item != article1)

    // 4、统计省份出现的次数
    val aggRDD: RDD[(String, Int)] = cleanRDD.reduceByKey(_ + _)

    // 5、统计出现次数最少的省份（聚合）
    val less: (String, Int) = aggRDD.sortBy(item => item._2, ascending = true).first() // first() : Action(操作)算子，Job执行了两个shuffle

    // 6、统计出现次数最多的省份（聚合）
    val more: (String, Int) = aggRDD.sortBy(item => item._2, ascending = false).first() // first() : Action(操作)算子，Job执行了两个shuffle

    println((less, more))
    /**
     * Driver : Spark的驱动器节点，负责运行Spark程序中的main方法，执行实际的代码。Driver在Spark作业时主要负责:
     * 1、将用户程序转化为作业（job）
     * 2、负责Executor之间的任务（task）调度
     * 3、监控Executor的执行状态
     * 4、通过UI展示运行情况，接收 Executor 计算后的结果
     *
     * Executor : 运行Spark作业中具体的任务，并且将执行结果返回给Driver
     *
     * job : 提交给spark的任务，spark调度的最大单位，每使用一个Action算子就会创建一个 job
     * stage : 每一个job处理过程要分为的几个阶段
     * task : 每一个job处理过程要分几为几次任务，Task是任务运行的最小单位，最终是要以task为单位运行在executor中
     * job —>（一对多） stage —>（一对多） task
     *
     * shuffle : 只有 k,v 型的数据才会有shuffle操作，对一个分区内的数据进行拆分发往不同的分区产生 shuffle
     *
     * Transformations(转换)算子的作用 : 生成RDD，以及RDD之间的依赖关系
     * Action(操作)算子 : 生成Job，去执行Job，全局执行了四个shuffle
     *
     * RDD的依赖：
     * 1、窄依赖 -> 没有 shuffle
     * 2、宽依赖 -> 有 shuffle
     */
  }

  @Test
  def cache(): Unit = {

    // RDD 的处理部分
    val source: RDD[String] = sc.textFile("dataset/hoteldata/jiudian.csv")
    val countRDD: RDD[(String, Int)] = source.map(item => (item.split(",")(3), 1))
    val article1: (String, Int) = countRDD.first()
    val cleanRDD: RDD[(String, Int)] = countRDD.filter(item => StringUtils.isNotEmpty(item._1) && item != article1)
    var aggRDD: RDD[(String, Int)] = cleanRDD.reduceByKey(_ + _)
    //将aggRDD放入缓存中，减少后面的shuffle
    aggRDD = aggRDD.cache()

    // 两个 RDD 的Action操作，每个Action都会完整运行一下 RDD 的整个依赖
    val less: (String, Int) = aggRDD.sortBy(item => item._2, ascending = true).first() // first() : Action(操作)算子，Job执行了两个shuffle
    val more: (String, Int) = aggRDD.sortBy(item => item._2, ascending = false).first() // first() : Action(操作)算子，Job执行了两个shuffle

    println((less, more))
  }

  @Test
  def persist(): Unit = {

    // RDD 的处理部分
    val source: RDD[String] = sc.textFile("dataset/hoteldata/jiudian.csv")
    val countRDD: RDD[(String, Int)] = source.map(item => (item.split(",")(3), 1))
    val article1: (String, Int) = countRDD.first()
    val cleanRDD: RDD[(String, Int)] = countRDD.filter(item => StringUtils.isNotEmpty(item._1) && item != article1)
    var aggRDD: RDD[(String, Int)] = cleanRDD.reduceByKey(_ + _)
    //将aggRDD放入缓存中，设置存储级别，减少后面的shuffle
    aggRDD = aggRDD.persist(StorageLevel.MEMORY_ONLY)

    // 两个 RDD 的Action操作，每个Action都会完整运行一下 RDD 的整个依赖
    val less: (String, Int) = aggRDD.sortBy(item => item._2, ascending = true).first() // first() : Action(操作)算子，Job执行了两个shuffle
    val more: (String, Int) = aggRDD.sortBy(item => item._2, ascending = false).first() // first() : Action(操作)算子，Job执行了两个shuffle

    println((less, more))
  }

  @Test
  def checkpoint(): Unit = {

    // 设置保存 checkpoint 的目录，也可以设置为 hdfs 上的目录
    sc.setCheckpointDir("dataset/checkpoint")

    // RDD 的处理部分
    val source: RDD[String] = sc.textFile("dataset/hoteldata/jiudian.csv")
    val countRDD: RDD[(String, Int)] = source.map(item => (item.split(",")(3), 1))
    val article1: (String, Int) = countRDD.first()
    val cleanRDD: RDD[(String, Int)] = countRDD.filter(item => StringUtils.isNotEmpty(item._1) && item != article1)
    var aggRDD: RDD[(String, Int)] = cleanRDD.reduceByKey(_ + _)

    // 不准确的说，checkpoint 是一个Action 操作，也就是说
    // 如果调用 checkpoint，则会重新计算一下整个RDD的依赖，然后把结果存在 hdfs 或者本地目录中
    // 所有应该在 checkpoint 之前进行一次 cache
    aggRDD = aggRDD.cache()

    // 斩断 RDD　的依赖链，将结果保存在　hdfs　上或本地目录中
    aggRDD.checkpoint()

    // 两个 RDD 的Action操作，每个Action都会完整运行一下 RDD 的整个依赖
    val less: (String, Int) = aggRDD.sortBy(item => item._2, ascending = true).first() // first() : Action(操作)算子，Job执行了两个shuffle
    val more: (String, Int) = aggRDD.sortBy(item => item._2, ascending = false).first() // first() : Action(操作)算子，Job执行了两个shuffle

    println((less, more))
  }

}
