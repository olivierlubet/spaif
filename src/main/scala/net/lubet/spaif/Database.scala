package net.lubet.spaif

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import Context.spark.implicits._

object Database {
  lazy val stockValue: DataFrame = {
    Context.spark.read.parquet("spark/stock_value").
      repartition($"ISIN").
      withColumn("Day_Count", row_number().over(Window.partitionBy($"ISIN").orderBy($"Date"))).
      cache()
  }
}

