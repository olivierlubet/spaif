package net.lubet.spaif

//import net.lubet.spaif._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs._

object Indicators {

  import Context.spark.implicits._
  import Context.spark._
  import Context._

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
  }

  def prepare = {

    add(closeIndic)
    add(movingAverage("Close", "XS", 3))
    add(movingAverage("Close", "S", 6))
    add(movingAverage("Close", "M", 12))
    add(movingAverage("Close", "L", 30))
    add(movingAverage("Close", "XL", 90))
    add(rate("S", "L"))
    add(rate("S", "M"))
    add(rate("M", "L"))
    add(rate("XS", "S"))
    add(rate("L", "XL"))

    add(performance("S", 5))
    add(performance("S", -1))
    add(performance("S", -5))
    add(performance("S", -15))
    add(performance("S", -30))

    add(derivative("S/M",-1))
    add(derivative("S/L",-1))
    add(derivative("M/L",-1))
    add(derivative("XS/S",-1))
    add(derivative("L/XL",-1))

    add(diff("P-S-1","P-S-5"))
    add(diff("P-S-5","P-S-15"))
    add(diff("P-S-15","P-S-30"))


    pivot().show
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
         |select isin,date,"P-$indicatorType${if (delay < 0) "" else "+"}$delay" type,val1/val2-1 value
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

  def diff(diminuende : String, diminutor : String): DataFrame = {
    //pivot(diminuende, diminutor).createOrReplaceTempView("tmp_pivot")
    sql(
      s"""
         |select i1.isin,i1.date,"($diminuende)-($diminutor)" as type, i1.value-i2.value as value
         |from indicator i1
         |join indicator i2 on i1.isin=i2.isin and i1.date=i2.date
         |where i1.type="$diminuende" and i2.type="$diminutor"
      """.stripMargin)
  }

  def rate(numerator: String, denominator: String): DataFrame = {
    pivot(numerator, denominator).createOrReplaceTempView("tmp_pivot")
    sql(
      s"""
         |select i.isin,i.date,"$numerator/$denominator" as type, i.$numerator/i.$denominator-1 as value
         |from tmp_pivot i
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
      """.stripMargin).show
  }

  def add(df: DataFrame) = {

    if (df.head(1).nonEmpty) {
      val index = df.head.fieldIndex("type")
      val indicatorType = df.head.getString(index)

      val ref = sql(s"""select isin, max(date) date  from indicator where type="$indicatorType" group by isin""")
      val adding = df.join(ref, ref("isin") === df("isin"), "left").filter(df("date") > ref("date") || ref("date").isNull).select(df("isin"),df("date"),df("value"),df("type"))
      println(s"Adding ${adding.count} rows")
      adding.write.mode(SaveMode.Append).partitionBy("isin", "type").saveAsTable("indicator")
    }
  }

  lazy val closeIndic = sql(
    """
      |select q.isin,q.date,"Close" type, q.Open Value
      |from quotation q
      |where q.isin in ("FR0000045072","FR0000130809")
      |--and q.date<"2010-01-01" -- for test
    """.stripMargin
  )
}