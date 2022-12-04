package sql.hotel

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.File

object HotelManagement_5 {
  def main(args: Array[String]): Unit = {
    //1、创建sparkSession
    val ssc: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("hotel_management_2")
      .getOrCreate()

    //2、数据读取
    val hotelData: DataFrame = ssc.read.option("header", value = true).csv("dataset/hoteldata/opt1")

    //导入隐式转换
    import ssc.implicits._

    //3、提取字段并去除NULL值
    val getHotel: Dataset[(String, Int)] = hotelData.map(hotel => {
      val hotels: Array[String] = hotel.toString().split(",")
      (hotels(3), hotels(21))
    }).filter(hotel => hotel._1 != "NULL" && hotel._2 != "NULL").map(hotel => (hotel._1, hotel._2.toInt))

    //4、排序
    val maximum: Array[(String, Int)] = getHotel.rdd.sortBy(hotel => hotel._2, ascending = false, 1).take(1)

    //调用工具类，删除目录
    DeleteDirUtils(new File("dataset/hoteldata/opt5"))

    //5、将结果输出到opt5目录中
    ssc.sparkContext.parallelize(maximum).repartition(1).saveAsTextFile("dataset/hoteldata/opt5")
  }
}
