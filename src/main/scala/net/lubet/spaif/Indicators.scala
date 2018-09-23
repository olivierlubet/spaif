package net.lubet.spaif

//import net.lubet.spaif._

import net.lubet.spaif.Indicators.classificationBinGt
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.spark.storage.StorageLevel

object Indicators {

  import Context.spark.implicits._
  import Context.spark._
  import Context._

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000 + "s")
    result
  }



  def compute = {

    sqlContext.clearCache()

    Database.initIndicator

    process(closeIndic)
    process(
      movingAverage("Close", "XS", 3),
      movingAverage("Close", "S", 6),
      movingAverage("Close", "M", 12),
      movingAverage("Close", "L", 30),
      movingAverage("Close", "XL", 90)
    )

    process(
      performance("XS", 15),
      performance("XS", 5),
      performance("XS", -1),
      performance("XS", -5),
      performance("XS", -15),
      performance("XS", -30)
    )

    process(
      derivative("P-XS-5", -1),
      derivative("P-XS-15", -1),
      derivative("P-XS-30", -1),

      diff("P-XS-1", "P-XS-5"),
      diff("P-XS-5", "P-XS-15"),
      diff("P-XS-15", "P-XS-30")
    )

    process(
      rate("XS", "S"),
      rate("XS", "L"),
      rate("S", "L"),
      rate("S", "M"),
      rate("M", "L"),
      rate("L", "XL")
    )
    process(
      derivative("XS/S", -1),
      derivative("XS/L", -1),
      derivative("S/M", -1),
      derivative("S/L", -1),
      derivative("M/L", -1),
      derivative("L/XL", -1)
    )

    process(
      classification(),
      classificationBinGt("P-XS+15", 6),
      classificationBinGt("P-XS+15", 5),
      classificationBinGt("P-XS+15", 4),
      classificationBinGt("P-XS+5", 3),
      classificationBinGt("P-XS+5", 2),
      classificationBinGt("P-XS+5", 1)

    )

    stats
  }

  def process(first: DataFrame, set: DataFrame*) = {
    time(add(set.foldLeft(first)((acc: DataFrame, ds: DataFrame) => {
      acc.union(ds)
    })))
  }

  def export = {
    time({
      println("write table data")
      sqlContext.clearCache()
      val data = pivot().cache()
      data.write.mode(SaveMode.Overwrite).saveAsTable("data")
      data.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("data/stock_value.csv")

      //data.schema.fields.foreach(f=>println(s""""${f.name}","""))

      val fs = FileSystem.get(Context.spark.sparkContext.hadoopConfiguration)
      val filePath = fs.globStatus(new Path("data/stock_value.csv/part*"))(0).getPath
      fs.rename(filePath, new Path("data.csv"))
    })
  }

  def classification(): DataFrame = {
    sql(
      """
        |select isin,date,
        |case
        | when value>0.05 then 5.0
        | when value>0.03 then 3.0
        | when value>0.01 then 1.0
        | else 0.0
        |end value,
        |"Class" type
        |--, value as test
        |from indicator
        | where type="P-S+5"
      """.stripMargin)
  }

  def classificationBinGt(from: String, threshold: Integer): DataFrame = {
    sql(
      s"""
         |select isin,date,
         |case
         | when (value*100)>$threshold then 1.0
         | else 0.0
         |end value,"gt${threshold}" type
         |from indicator
         | where type="$from"
    """.stripMargin)
  }

  /**
    *
    * @param indicatorType
    * @param delay negative means look in the past
    * @return
    */
  def performance(indicatorType: String, delay: Integer): DataFrame = {
    sql(
      s"""
         |select isin,
         |date,
         |${if (delay < 0) "val1/val2-1" else "val2/val1-1"} value,
         |"P-$indicatorType${if (delay < 0) "" else "+"}$delay" type
         |from (
         |select isin,date, value as val1,
         |LAG(value,${-delay}) OVER (PARTITION BY i.isin ORDER BY i.date ASC) AS val2
         |from indicator i
         |where i.type="$indicatorType"
         |-- and date < "2010-01-01" -- for tests
         |) where val2 is not null
      """.stripMargin)
  }

  /**
    *
    * @param indicatorType
    * @param delay negative means look in the past
    * @return
    */
  def derivative(indicatorType: String, delay: Integer): DataFrame = {
    sql(
      s"""
         |select isin,
         |date,
         |val1-val2 value,
         |"D-$indicatorType${if (delay < 0) "" else "+"}$delay" type
         |from (
         |select isin,date, value as val1,
         |LAG(value,${-delay}) OVER (PARTITION BY i.isin ORDER BY i.date ASC) AS val2
         |from indicator i
         |where i.type="$indicatorType"
         |-- and date < "2010-01-01" -- for tests
         |) where val2 is not null
      """.stripMargin)
  }

  def diff(diminuende: String, diminutor: String): DataFrame = {
    sql(
      s"""
         |select i1.isin,
         |i1.date,
         |i1.value-i2.value as value,
         |"$diminuende-$diminutor" as type
         |from indicator i1
         |join indicator i2 on i1.isin=i2.isin and i1.date=i2.date
         |where i1.type="$diminuende" and i2.type="$diminutor"
      """.stripMargin)
  }

  def rate(numerator: String, denominator: String): DataFrame = {
    sql(
      s"""
         |select i1.isin,
         |i2.date,
         |i1.value/i2.value-1 as value,
         |"$numerator/$denominator" as type
         |from indicator i1
         |join indicator i2 on i1.isin=i2.isin and i1.date=i2.date
         |where i1.type="$numerator" and i2.type="$denominator"
      """.stripMargin)
  }

  def movingAverage(from: String, to: String, delay: Integer): DataFrame = {
    assert(delay >= 1)
    sql(
      s"""
         |SELECT i.isin,i.date,
         |AVG(i.value)
         |OVER (PARTITION BY i.isin ORDER BY i.date ASC ROWS ${delay - 1} PRECEDING) AS value,
         |"$to" as type
         |FROM   indicator i
         |where i.type="$from"
         |-- and i.date < "2010-01-01" -- uncomment for tests
      """.stripMargin
    )
  }

  lazy val closeIndic = sql(
    """
      |select q.isin,q.date, q.Open value,
      |"Close" type
      |from quotation q
      |--where q.isin in ("FR0000045072","FR0000130809")
      |--and q.date<"2010-01-01" -- for test
    """.stripMargin
  )

  def pivot(columnList: String*) = {
    val df = sql(
      """
        |select * from indicator
      """.stripMargin
    ).groupBy("isin", "date")
    if (columnList.isEmpty) df.pivot("type").agg(sum($"value"))
    else df.pivot("type", columnList).agg(sum($"value"))
  }

  def stats = {
    sql(
      """
        |select type,count(1) from indicator group by type order by type
      """.stripMargin).show(100)
  }

  def add(df: DataFrame) = {
    df.write.mode(SaveMode.Append).partitionBy("type").saveAsTable("indicator")
  }

  lazy val maxDates = sql(s"""select isin, type, max(date) date from indicator group by isin,type""").persist(StorageLevel.MEMORY_ONLY)

}