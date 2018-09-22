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
  }

  def quotation: DataFrame = {
    Context.spark.table("quotation").cache
  }

  def data: DataFrame = {
    Context.spark.table("data").cache
  }

  def indicator: DataFrame = {
    Context.spark.table("indicator").cache
  }

  def initIndicator: DataFrame = {
    sql(
      """
      DROP TABLE IF EXISTS indicator
      """)

    sql(
      """
      CREATE TABLE  if not exists indicator
      (isin string, type string, date date, value double)
      USING parquet
      PARTITIONED BY (Type)
      """)

  }

  def initWatchlist: DataFrame = {
    sql(
      """
      DROP TABLE IF EXISTS watchlist
      """)

    sql(
      """
      CREATE TABLE  if not exists watchlist
      (isin string)
      USING parquet
      """)
  }

  def insertWatchlist(isin: String): DataFrame = sql(
    s"""
       |insert into watchlist values ("${isin}")
     """.stripMargin)

  def lastQuotation: DataFrame = {
    Database.quotation.groupBy("ISIN").agg(max("Date").alias("last_quotation"))
  }

  def alter: DataFrame = {
    sql(
      """
        |ALTER TABLE indicator DROP PARTITION (isin="__HIVE_DEFAULT_PARTITION__")
      """.stripMargin)

    sql( """ALTER TABLE indicator DROP PARTITION (type = "gt9")""")
  }
}

