package net.lubet.spaif

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import Context.spark.implicits._
import org.apache.spark.sql.types._

object Serie {

  //exponential moving average
  def EMA(df: DataFrame, from: String, to: String, rate: Double = 0.2): DataFrame = {
    val indexFrom = df.head().fieldIndex(from)
    val indexTo=df.head().size

    def partEMA(part: Iterator[Row]): Iterator[Row] = {

      part.foldLeft(List[Row]()) { (acc: List[Row], r: Row) =>
        if (acc.isEmpty) {
          Row.fromSeq(r.toSeq :+ r.getDouble(indexFrom)) :: acc
        } else {
          val prevVal = acc.head.getDouble(indexTo)
          val newVal = r.getDouble(indexFrom) * rate + prevVal * (1-rate)
          Row.fromSeq(r.toSeq :+ newVal) :: acc
        }

      }.reverseIterator
    }

    val newSchema = StructType(df.schema.fields ++ Array(StructField(to, DoubleType, false)))
    Context.spark.createDataFrame(df.rdd.mapPartitions{lines => partEMA(lines)},newSchema)
  }
}
