import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class Demo {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("world")
    .master("local[3]")
    .getOrCreate()

  @Test
  def song(): Unit = {

    val data: DataFrame = sparkSession.read.option("sep", " ")
      .csv("dataset/demodata/user_artist_data.txt").toDF("user_id", "song_id", "num")

    data.createOrReplaceTempView("song")

    //统计非重复的用户个数
    sparkSession.sql("select count(distinct user_id) as num from song").show()

    //统计用户听过的歌曲总数
    sparkSession.sql("select user_id,count(user_id) as num from song group by user_id").show()

    //找出 ID 为“1000002”的用户喜欢的 10 首歌曲（即播放次数多的 10 首歌曲）
    sparkSession.sql("select * from song where user_id='1000002' order by num desc limit 10").show()

  }

  @Test
  def businessAnalysis(): Unit = {

    val tbDate: DataFrame = sparkSession.read.option("header", true).csv("dataset/demodata/tbDate.txt")
    val tbStock: DataFrame = sparkSession.read.option("header", true).csv("dataset/demodata/tbStock.txt")
    val tbStockDetail: DataFrame = sparkSession.read.option("header", true).csv("dataset/demodata/tbStockDetail.txt")

    tbDate.createOrReplaceTempView("tbDate")
    tbStock.createOrReplaceTempView("tbStock")
    tbStockDetail.createOrReplaceTempView("tbStockDetail")

    // 1、计算所有订单中每年的销售单数，销售总额
    sparkSession.sql(
      """
        |SELECT
        |	td.Theyear AS date,
        |	SUM( cast(tsd.Qty as float) ) AS sumQty,
        |	SUM( cast(tsd.Amount as float) ) AS sumAmount
        |FROM
        |	tbStock ts
        |	JOIN tbDate td ON td.DateId = ts.DateId
        |	JOIN tbStockDetail tsd ON ts.Ordernumber = tsd.Ordernumber
        |GROUP BY
        |	date
        |ORDER BY
        |	date
      """.stripMargin).show()

    // 2、计算所有订单每年最大金额订单的销售额
    sparkSession.sql(
      """
        |SELECT
        |	td.Theyear AS date,
        |	max(cast(tsd.Amount as float)) AS maxAmount
        |FROM
        |	tbStock ts
        |	JOIN tbDate td ON td.DateId = ts.DateId
        |	JOIN tbStockDetail tsd ON ts.Ordernumber = tsd.Ordernumber
        |GROUP BY
        |	date
        |ORDER BY
        |	date
      """.stripMargin).show()

    // 3、计算所有订单中每年最畅销货品
    sparkSession.sql(
      """
        |SELECT
        |	je.Theyear,
        |	je.Itemid
        |FROM
        |	(
        |	SELECT
        |		td.Theyear,
        |		tsd.Itemid,
        |		tsd.Amount
        |	FROM
        |		tbStock AS ts
        |		INNER JOIN tbStockDetail tsd ON ts.Ordernumber = tsd.Ordernumber
        |		INNER JOIN tbDate td ON td.DateId = ts.DateId
        |	) je
        |	JOIN (
        |	SELECT
        |		td.Theyear,
        |		max( cast( tsd.Amount AS FLOAT ) ) AS Amount
        |	FROM
        |		tbStock AS ts
        |		INNER JOIN tbStockDetail tsd ON ts.Ordernumber = tsd.Ordernumber
        |		INNER JOIN tbDate td ON td.DateId = ts.DateId
        |	GROUP BY
        |		td.Theyear
        |	) zdje ON je.Theyear = zdje.Theyear
        |WHERE
        |	je.Amount = zdje.Amount
        |ORDER BY
        |	zdje.Theyear
      """.stripMargin).show()
  }

}

