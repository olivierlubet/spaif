package net.lubet.spaif

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import Context.spark._
import Context.spark.implicits._

object Database {

  def createView(): Unit = {
    stock.createOrReplaceTempView("stock")
    quotation.createOrReplaceTempView("quotation")
    data.createOrReplaceTempView("data")
  }

  def stock: DataFrame = {
    Context.spark.table("stock").cache

    /*Context.spark.read.parquet("spark/stock").
      repartition($"ISIN").
      cache()*/
  }

  def quotation: DataFrame = {
    Context.spark.table("quotation").cache

    /*Context.spark.read.parquet("spark/quotation").
      repartition($"ISIN").
      cache()*/
    //withColumn("Day_Count", row_number().over(Window.partitionBy($"ISIN").orderBy($"Date"))).
  }

  def data: DataFrame = {
    Context.spark.table("data").cache
    /*Context.spark.read.parquet("spark/data").
      repartition($"ISIN").
      cache*/
  }

  def indicator: DataFrame = {
    Context.spark.table("indicator").cache
  }

  def lastQuotation: DataFrame = {
    Database.quotation.groupBy("ISIN").agg(max("Date").alias("last_quotation"))
  }
}

