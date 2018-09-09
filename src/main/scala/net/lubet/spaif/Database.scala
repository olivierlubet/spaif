package net.lubet.spaif

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import Context.spark._
import Context.spark.implicits._

object Database {
  lazy val stockValue: DataFrame = {
    //sql("select * from quotation").cache

    Context.spark.read.parquet("spark/quotation").
      repartition($"ISIN").
      //withColumn("Day_Count", row_number().over(Window.partitionBy($"ISIN").orderBy($"Date"))).
      cache()

  }
}

