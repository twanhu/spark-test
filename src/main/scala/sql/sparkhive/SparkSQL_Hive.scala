package sql.sparkhive

import org.apache.spark.sql.SparkSession

object SparkSQL_Hive {
  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val spark: SparkSession = SparkSession.builder().enableHiveSupport()
      .master("local[3]")
      .appName("sparkSQL_Hive")
      .getOrCreate()

    spark.sql("use gmall")
    spark.sql("show tables").show()

    // 关闭环境
    spark.close()
  }
}
