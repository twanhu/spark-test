package sql.hotel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io._

object HotelManagement_1 {
  def main(args: Array[String]): Unit = {
    //1、创建sparkSession
    val ssc: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("hotel_management_1")
      .getOrCreate()

    //2、数据读取
    val hotelData: RDD[String] = ssc.sparkContext.textFile("dataset/hoteldata/jiudian.csv")

    //3、数据清洗
    val cleanHotelData: RDD[String] = hotelData.filter(hotel => {
      //分割字符串
      val hotels: Array[String] = hotel.split(",")
      //定义一个累加器，统计NULL出现的次数
      var a: Int = 0
      //遍历数组，统计NULL的个数
      hotels.foreach(hotels => {
        if (hotels == "NULL") {
          a += 1
        }
      })
      a < 3
    })

    //调用工具类，删除目录
    DeleteDirUtils(new File("dataset/hoteldata/opt1"))

    //4、将结果输出到opt1目录中
    cleanHotelData.repartition(1).saveAsTextFile("dataset/hoteldata/opt1")
  }
}