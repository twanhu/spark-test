package sql.hotel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

object HotelManagement_2 {
  def main(args: Array[String]): Unit = {
    //1、创建sparkSession
    val ssc: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("hotel_management_2")
      .getOrCreate()

    //2、数据读取
    val hotelData: RDD[String] = ssc.sparkContext.textFile("dataset/hoteldata/opt1")

    //导入隐式转换
    import ssc.implicits._

    //3、提取省份字段并聚和
    val hotelNumber: DataFrame = hotelData.map(hotel => {
      val hotels: Array[String] = hotel.split(",")
      (hotels(3), 1)
    }).reduceByKey(_ + _).map(hotel => HotelStatistics(hotel._1, hotel._2))
      .filter(hotel => hotel.province != "省份" && hotel.province != "NULL").toDF("省份", "个数")

    //调用工具类，删除目录
    DeleteDirUtils(new File("dataset/hoteldata/opt2"))

    //4、将结果输出到opt2目录中
    hotelNumber.repartition(1).write.csv("dataset/hoteldata/opt2")
    hotelNumber.show()

  }
}

case class HotelStatistics(province: String, number: Int)
