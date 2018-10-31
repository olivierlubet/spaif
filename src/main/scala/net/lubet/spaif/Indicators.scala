package net.lubet.spaif

//import net.lubet.spaif._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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

    process(closeIndic, openIndic, lowIndic, highIndic)
    process(
      movingAveragePlus("Close", "XS", 3),
      movingAveragePlus("Close", "S", 6),
      movingAveragePlus("Close", "M", 12),
      movingAveragePlus("Close", "L", 30),
      movingAveragePlus("Close", "XL", 90),
      movingAveragePlus("Close", "XXL", 200),

      movingAveragePlus("Open", "XS-O", 3),
      movingAveragePlus("High", "XS-H", 3),
      movingAveragePlus("Low", "XS-L", 3)
    )

    process(
      stdDeviation("Close", 10),
      stdDeviation("XS", 10),
      stdDeviation("M", 10),
      performance("Close", -1),
      performance("Close", +1),
      performance("Close", +5),
      performance("XS", 30),
      performance("XS", 15),
      performance("XS", 5),
      performance("XS", -1),
      performance("XS", -5),
      performance("XS", -15),
      performance("XS", -30)
    )

    process(
      bollinger("Close", "STDDEV-Close-10"),
      bollinger("XS", "STDDEV-XS-10"),
      bollinger("M", "STDDEV-M-10"),

      derivative("P-XS-5", -1),
      derivative("P-XS-15", -1),
      derivative("P-XS-30", -1),

      diff("P-XS-1", "P-XS-5"),
      diff("P-XS-5", "P-XS-15"),
      diff("P-XS-15", "P-XS-30")
    )

    process(
      rate("Close", "BOL-H-Close"),
      rate("Close", "BOL-L-Close"),
      rate("XS", "BOL-H-XS"),
      rate("XS", "BOL-L-XS"),
      rate("M", "BOL-H-M"),
      rate("M", "BOL-L-M"),

      rate("Close", "Open"),
      rate("High", "Low"),
      rate("XS", "XS-O"),
      rate("XS", "XS-H"),
      rate("XS", "XS-L"),

      rate("XS", "S"),
      rate("XS", "L"),
      rate("S", "L"),
      rate("S", "M"),
      rate("M", "L"),
      rate("L", "XL"),
      rate("XL", "XXL")
    )

    process(
      derivative("XS/XS-O", -1),
      derivative("XS/XS-H", -1),
      derivative("XS/XS-L", -1),
      derivative("XS/S", -1),
      derivative("XS/L", -1),
      derivative("S/M", -1),
      derivative("S/L", -1),
      derivative("M/L", -1),
      derivative("L/XL", -1)
    )

    process(
      classification(),
      classificationBinGt("P-Close+5", 3),
      classificationBinGt("P-Close+5", 2),
      classificationBinGt("P-Close+5", 1)

    )

    stats

    pivot().write.mode(SaveMode.Overwrite).saveAsTable("data")
  }

  def process(first: DataFrame, set: DataFrame*) = {
    time(add(set.foldLeft(first)((acc: DataFrame, ds: DataFrame) => {
      acc.union(ds)
    })))
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
        | where type="P-XS+15"
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

  def bollinger(from: String, stddev: String): DataFrame = {
    sql(
      s"""
         |SELECT i.isin,i.date,
         |i.value+i2.value*2 as value,
         |"BOL-H-${from}" as type
         |FROM indicator i
         |JOIN indicator i2 on i2.type="${stddev}" and i.isin=i2.isin and i.date=i2.date
         |WHERE i.type="${from}"
         |
         |UNION
         |
         |SELECT i.isin,i.date,
         |i.value-i2.value*2 as value,
         |"BOL-L-${from}" as type
         |FROM indicator i
         |JOIN indicator i2 on i2.type="${stddev}" and i.isin=i2.isin and i.date=i2.date
         |WHERE i.type="${from}"
      """.stripMargin)
  }

  def stdDeviation(from: String, delay: Integer): DataFrame = {
    sql(
      s"""
         |SELECT i.isin,i.date,
         |STDDEV(i.value)
         |OVER (PARTITION BY i.isin ORDER BY i.date ASC ROWS ${delay} PRECEDING) AS value,
         |"STDDEV-${from}-${delay}" as type
         |FROM   indicator i
         |where i.type="$from"
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

  def movingAveragePlus(from: String, to: String, delay: Integer): DataFrame = {
    assert(delay >= 1)
    sql(
      s"""
         |SELECT i.isin,i.date,
         |(AVG(i.value) OVER (PARTITION BY i.isin ORDER BY i.date ASC ROWS ${delay - 1} PRECEDING)) * ${delay - 1} / ${delay} +
         | i.value / ${delay}
         | AS value,
         |"$to" as type
         |FROM   indicator i
         |where i.type="$from"
         |-- and i.date < "2010-01-01" -- uncomment for tests
      """.stripMargin
    )
  }

  lazy val closeIndic = sql(""" select q.isin,q.date, q.Close value, "Close" type from quotation q """)
  lazy val openIndic = sql(""" select q.isin,q.date, q.Open value, "Open" type from quotation q """)
  lazy val lowIndic = sql(""" select q.isin,q.date, q.Low value, "Low" type from quotation q """)
  lazy val highIndic = sql(""" select q.isin,q.date, q.High value, "High" type from quotation q """)

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