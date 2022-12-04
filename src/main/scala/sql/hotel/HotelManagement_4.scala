package sql.hotel

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.File

object HotelManagement_4 {
  def main(args: Array[String]): Unit = {
    //1、创建sparkSession
    val ssc: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("hotel_management_4")
      .getOrCreate()

    //2、数据读取
    val gethotelData: DataFrame = ssc.read.option("header", value = true).csv("dataset/hoteldata/opt1")

    //导入隐式转换
    import ssc.implicits._

    //3、提取省份、酒店实住间夜、酒店总间夜字段,进行数据清洗,去除包含NULL的数据
    val hotelData: Dataset[(String, (String, String))] = gethotelData.map(hotel => {
      val hotels: Array[String] = hotel.toString().split(",")
      (hotels(3), (hotels(14), hotels(16)))
    }).filter(hotel => hotel._1 != "NULL" && hotel._2._1 != "NULL" && hotel._2._2 != "NULL")

    //4、按照省份聚合，统计结果，求出平均酒店实住间夜率
    val getRealOccupancyRate: DataFrame = hotelData.map(a => (a._1, (a._2._1.toDouble, a._2._2.toDouble))).rdd
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(hotel => HotelRealOccupancyRate(hotel._1, ((hotel._2._2 / hotel._2._1) * 100).formatted("%.2f") + "%"))
      .toDF("省份", "平均酒店实住间夜率")

    //调用工具类，删除目录
    DeleteDirUtils(new File("dataset/hoteldata/opt4"))

    //5、将结果输出到opt4目录中
    getRealOccupancyRate.repartition(1).write.csv("dataset/hoteldata/opt4")
    getRealOccupancyRate.show()

  }
}

case class HotelRealOccupancyRate(province: String, realOccupancyRate: String)