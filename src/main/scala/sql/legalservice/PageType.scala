package sql.legalservice

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object PageType {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("page_type")
      .getOrCreate()

    //指定mysql连接地址
    val url = "jdbc:mysql://localhost:3306/leader"

    //指定要加载的表名
    val tableName = "law"

    //配置连接数据库的相关属性
    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    val dataFrame: DataFrame = sparkSession.read.jdbc(url, tableName, properties)

    dataFrame.createOrReplaceTempView("law")

    /**
     * substring(string, position, length) : string原字符串（字段名），position指的是从哪个位置开始截取子字符串，length指截取的长度
     * round(x,d) : x指要处理的数，d是指保留几位小数
     * count() : 按照条件聚合
     * like : 模糊查找
     * group by : 分组
     */
    /*sparkSession.sql(
      "select substring(page_type, 1, 3) as page_type, count(*) as number_of_records," +
        " round(count(*)/837456.0*100, 2) as percentage from law group by substring(page_type,1,3)")
      .show(7)*/
    /*sparkSession.sql("select page_type, count(*) as count_num from law" +
      " where visiturl like '%faguizt%' and page_type like '%199%' group by page_type").show()*/
    /*sparkSession.sql("select page_type,count(*) as count_num,round((count(*)/411665.0) * 100,2) as weights from" +
      " law where substring(page_type,1,3)=101 group by page_type").show()*/
    //查看包含”?“总记录数
//    sparkSession.sql("select count(*) as num from law where visiturl like '%?%'").show()
    sparkSession.sql("select page_type, count(*) as count_num, round((count(*)*100)/65477.0,2) as weights from" +
      " law where visiturl like '%?%' group by page_type").show()

  }

}
