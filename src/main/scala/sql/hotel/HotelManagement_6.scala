package sql.hotel

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.File

object HotelManagement_6 {
  def main(args: Array[String]): Unit = {
    //1、创建sparkSession
    val ssc: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("hotel_management_6")
      .getOrCreate()

    //2、数据读取
    val gethotelData: DataFrame = ssc.read.option("header", value = true).csv("dataset/hoteldata/opt1")

    //导入隐式转换
    import ssc.implicits._

    //3、提取字段，去除包含NULL的数据
    val hotelData: Dataset[(String, (String, String, String, String))] = gethotelData.map(hotel => {
      val hotels: Array[String] = hotel.toString().split(",")
      (hotels(4), (hotels(19), hotels(20), hotels(17), hotels(18)))
    }).filter(hotel => hotel._1 != "NULL" && hotel._2._1 != "NULL" && hotel._2._2 != "NULL" && hotel._2._3 != "NULL" && hotel._2._4 != "NULL")

    //4、按照城市聚合，统计结果，求出酒店直销成功率
    val getHotelSuccessRate: DataFrame = hotelData.map(a => (a._1, (a._2._1.toDouble, a._2._2.toDouble, a._2._3.toDouble, a._2._4.toDouble))).rdd
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
      .map(hotel => SuccessRateHotelDirect(hotel._1, (((hotel._2._1 + hotel._2._2) / (hotel._2._3 + hotel._2._4)) * 100).formatted("%.2f") + "%"))
      .toDF("城市", "酒店直销成功率")

    //调用工具类，删除目录
    DeleteDirUtils(new File("dataset/hoteldata/opt6"))

    //5、将结果输出到opt6目录中
    getHotelSuccessRate.repartition(1).write.csv("dataset/hoteldata/opt6")
    getHotelSuccessRate.show()

  }
}

case class SuccessRateHotelDirect(city: String, successRate: String)