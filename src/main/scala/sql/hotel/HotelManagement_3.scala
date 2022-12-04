package sql.hotel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object HotelManagement_3 {
  def main(args: Array[String]): Unit = {
    //1、创建sparkSession
    val ssc: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("hotel_management_3")
      .getOrCreate()

    //2、数据读取
    val hotelData: RDD[String] = ssc.sparkContext.textFile("dataset/hoteldata/opt1")

    //导入隐式转换
    import ssc.implicits._

    //3、提取星级字段并聚会
    val starRatingNumber: DataFrame = hotelData.map(hotel => {
      val hotels: Array[String] = hotel.split(",")
      val starRating: String = hotels(6) match {
        case "五星级/豪华" => "五星级"
        case "四星级/高档" => "四星级"
        case "三星级/舒适" => "三星级"
        case "二星及其他" => "二星级"
        case _ => "其他"
      }
      (starRating, 1)
    }).reduceByKey(_ + _).map(hotel => {
      StarRatingStatistics(hotel._1, hotel._2)
    }).toDF("星级", "个数")

    //调用工具类，删除目录
    DeleteDirUtils(new File("dataset/hoteldata/opt3"))

    //4、将结果输出到opt3目录中
    starRatingNumber.repartition(1).write.csv("dataset/hoteldata/opt3")
    starRatingNumber.show()

  }
}

case class StarRatingStatistics(starRating: String, number: Int)