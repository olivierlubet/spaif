package net.lubet.spaif

//import net.lubet.spaif._

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

  def init = {
    sql(
      """
      DROP TABLE IF EXISTS indicator
      """)

    sql(
      """
      CREATE TABLE  if not exists indicator
      (isin string, type string, date date, value double)
      USING parquet
      PARTITIONED BY (ISIN , Type )
      """)

    sql(
      """
        |ALTER TABLE indicator DROP PARTITION (isin="__HIVE_DEFAULT_PARTITION__")
      """.stripMargin)

    sql(
      """
        |ALTER TABLE indicator DROP PARTITION (type = "gt0.02")
      """.stripMargin)

  }

  def compute = {

    sqlContext.clearCache()

    time(add(closeIndic))
    time(add(movingAverage("Close", "XS", 3)))
    time(add(movingAverage("Close", "S", 6)))
    time(add(movingAverage("Close", "M", 12)))
    time(add(movingAverage("Close", "L", 30)))
    time(add(movingAverage("Close", "XL", 90)))
    time(add(rate("S", "L")))
    time(add(rate("S", "M")))
    time(add(rate("M", "L")))
    time(add(rate("XS", "S")))
    time(add(rate("L", "XL")))

    time(add(performance("S", 5)))
    time(add(performance("S", -1)))
    time(add(performance("S", -5)))
    time(add(performance("S", -15)))
    time(add(performance("S", -30)))

    time(add(derivative("P-S-5", -1)))
    time(add(derivative("P-S-15", -1)))
    time(add(derivative("P-S-30", -1)))
    time(add(derivative("S/M", -1)))
    time(add(derivative("S/L", -1)))
    time(add(derivative("M/L", -1)))
    time(add(derivative("XS/S", -1)))
    time(add(derivative("L/XL", -1)))

    time(add(diff("P-S-1", "P-S-5")))
    time(add(diff("P-S-5", "P-S-15")))
    time(add(diff("P-S-15", "P-S-30")))

    time(add(classification()))
    time(add(classificationBinGt("P-S+5", 5)))
    time(add(classificationBinGt("P-S+5", 4)))
    time(add(classificationBinGt("P-S+5", 3)))
    time(add(classificationBinGt("P-S+5", 2)))
    time(add(classificationBinGt("P-S+5", 1)))
  }

  def export={
    time({
      println("write table data")
      sqlContext.clearCache()
      val data = pivot().cache()
      data.write.mode(SaveMode.Overwrite).saveAsTable("data")
      data.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("data/stock_value.csv")

      val fs = FileSystem.get(Context.spark.sparkContext.hadoopConfiguration)
      val filePath = fs.globStatus(new Path("data/stock_value.csv/part*"))(0).getPath()
      fs.rename(filePath, new Path("data.csv"))
    })
  }

  def classification(): DataFrame = {
    sql(
      """
        |select isin,date,"Class" type,
        |case
        | when value>0.05 then 5.0
        | when value>0.03 then 3.0
        | when value>0.01 then 1.0
        | else 0.0
        |end value
        |--, value as test
        |from indicator
        | where type="P-S+5"
      """.stripMargin)
  }

  def classificationBinGt(from: String, threshold: Integer): DataFrame = {
    sql(
      s"""
         |select isin,date,"gt${threshold}" type,
         |case
         | when (value*100)>$threshold then 1.0
         | else 0.0
         |end value
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
         |select isin,date,"P-$indicatorType${if (delay < 0) "" else "+"}$delay" type,${if (delay < 0) "val1/val2-1" else "val2/val1-1"} value
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
         |select isin,date,"D-$indicatorType${if (delay < 0) "" else "+"}$delay" type,val1-val2 value
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
    //pivot(diminuende, diminutor).createOrReplaceTempView("tmp_pivot")
    sql(
      s"""
         |select i1.isin,i1.date,"$diminuende-$diminutor" as type, i1.value-i2.value as value
         |from indicator i1
         |join indicator i2 on i1.isin=i2.isin and i1.date=i2.date
         |where i1.type="$diminuende" and i2.type="$diminutor"
      """.stripMargin)
  }

  def rate(numerator: String, denominator: String): DataFrame = {
    //pivot(numerator, denominator).createOrReplaceTempView("tmp_pivot") // <-- A changer !!!
    sql(
      s"""
         |select i1.isin,i2.date,"$numerator/$denominator" as type, i1.value/i2.value-1 as value
         |from indicator i1
         |join indicator i2 on i1.isin=i2.isin and i1.date=i2.date
         |where i1.type="$numerator" and i2.type="$denominator"
      """.stripMargin)
  }

  def movingAverage(from: String, to: String, delay: Integer): DataFrame = {
    assert(delay >= 1)
    sql(
      s"""
         |SELECT i.ISIN,i.Date,"$to" as type,
         |AVG(i.value)
         |OVER (PARTITION BY i.isin ORDER BY i.date ASC ROWS ${delay - 1} PRECEDING) AS value
         |FROM   indicator i
         |where i.type="$from"
         |-- and i.date < "2010-01-01" -- uncomment for tests
      """.stripMargin
    )

  }

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
        |select isin,type,count(1) from indicator group by isin,type order by isin,type
      """.stripMargin).show(100)
  }

  def add(df: DataFrame) = {

    if (df.head(1).nonEmpty) {

      val index = df.head.fieldIndex("type")
      val indicatorType = df.head.getString(index)

      val ref = maxDates.filter($"type" === indicatorType)
      //val adding = df.join(ref, ref("isin") === df("isin"), "left").filter(df("date") > ref("date") || ref("date").isNull).select(df("isin"),df("date"),df("value"),df("type"))
      val adding = df.join(ref, ref("isin") === df("isin"), "left").filter(df("date") > ref("date") || ref("date").isNull).select(df("isin"), df("date"), df("value"), df("type"))

      println(s"Adding rows for $indicatorType") //${adding.count}
      adding.write.mode(SaveMode.Append).partitionBy("isin", "type").saveAsTable("indicator")
    }
  }

  lazy val maxDates = sql(s"""select isin, type, max(date) date from indicator group by isin,type""").persist(StorageLevel.MEMORY_ONLY)

  lazy val closeIndic = sql(
    """
      |select q.isin,q.date,"Close" type, q.Open Value
      |from quotation q
      |--where q.isin in ("FR0000045072","FR0000130809")
      |--and q.date<"2010-01-01" -- for test
    """.stripMargin
  )
}