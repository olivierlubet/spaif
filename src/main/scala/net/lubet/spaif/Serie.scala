package net.lubet.spaif

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import Context.spark.implicits._
import org.apache.spark.sql.types._

// Deal with "error: Unable to find encoder for type stored in a Dataset."
import org.apache.spark.sql.catalyst.encoders.RowEncoder

/*
TODO : écrire les données calculées dans une table spécifique / retourner uniquemenet ISIN DATE et la valeur calculée

objectif : disposer des indicateurs non nuls dans une table pour les incrémenter

https://stackoverflow.com/questions/38487667/overwrite-specific-partitions-in-spark-dataframe-write-method
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
data.write.mode("overwrite").insertInto("partitioned_table")

 */

object Serie {

  //import scala.reflect.ClassTag
  //implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
  //  org.apache.spark.sql.Encoders.kryo[A](ct)

  //
  /**
    * Usage EMA(stockValue,"Open","Open_",0.2).show
    *
    * @param df
    * @param from
    * @param to
    * @param rate
    * @return DataFrame with one column containing exponential moving average
    */
  def ema(df: DataFrame, from: String, to: String, rate: Double = 0.2): DataFrame = {
    val indexFrom = df.head().fieldIndex(from)
    val indexTo = df.head().size

    def part(lines: Iterator[Row]): Iterator[Row] = {

      lines.foldLeft(List[Row]()) { (acc: List[Row], r: Row) =>

        if (r.get(indexFrom) == null) {
          Row.fromSeq(r.toSeq :+ null) :: acc
        } else if (acc.isEmpty || acc.head.get(indexTo) == null) {
          Row.fromSeq(r.toSeq :+ r.getDouble(indexFrom)) :: acc
        } else {
          val prevVal = acc.head.getDouble(indexTo)
          val newVal = r.getDouble(indexFrom) * rate + prevVal * (1 - rate)
          Row.fromSeq(r.toSeq :+ newVal) :: acc
        }

      }.reverseIterator
    }

    val newSchema = StructType(df.schema.fields ++ Array(StructField(to, DoubleType, true)))
    df.repartition($"ISIN").mapPartitions(lines => part(lines))(RowEncoder.apply(newSchema))
  }

  def performance(df: DataFrame, from: String, to: String, delay: Integer): DataFrame = {
    val indexFrom = df.head().fieldIndex(from)

    def part(lines: Iterator[Row]): Iterator[Row] = {
      val list = lines.toList
      //println(list.size)

      val (before, linesFrom) = list.splitAt(if (delay < 0) -delay else 0)
      val linesTo = list.drop(if (delay > 0) delay else 0)
      val after = list.takeRight(if (delay > 0) delay else 0)

      {
        before.map(r => Row.fromSeq(r.toSeq :+ null)) ++
          (linesFrom zip linesTo).par.map {
            case (f:Row, t:Row) => {
              val newVal = if (f.get(indexFrom) == null || t.get(indexFrom) == null || f.getDouble(indexFrom) == 0d || t.getDouble(indexFrom) == 0d) {
                0d
              } else {
                f.getDouble(indexFrom) / t.getDouble(indexFrom) - 1
              }
              Row.fromSeq(f.toSeq :+ newVal)
            }
          } ++
          after.map(r => Row.fromSeq(r.toSeq :+ null))
      }.toIterator
    }

    val newSchema = StructType(df.schema.fields ++ Array(StructField(to, DoubleType, true)))
    df.repartition($"ISIN").mapPartitions(lines => part(lines))(RowEncoder.apply(newSchema))
  }

  def rate(df: DataFrame, from1: String, from2: String, to: String): DataFrame = {
    val indexFrom1 = df.head().fieldIndex(from1)
    val indexFrom2 = df.head().fieldIndex(from2)

    val newSchema = StructType(df.schema.fields ++ Array(StructField(to, DoubleType, false)))

    Context.spark.createDataFrame(df.rdd.map { r: Row =>
      val newVal = if (r.get(indexFrom1) == null || r.get(indexFrom2) == null || r.getDouble(indexFrom2) == 0) {
        0d
      } else {
        r.getDouble(indexFrom1) / r.getDouble(indexFrom2) - 1
      }

      Row.fromSeq(r.toSeq :+ newVal)
    }, newSchema)
  }

  def derivative(df: DataFrame, from: String, to: String, delay: Integer): DataFrame = {

    val indexFrom = df.head().fieldIndex(from)

    def part(lines: Iterator[Row]): Iterator[Row] = {
      val list = lines.toList
      //println(list.size)

      val (before, linesFrom) = list.splitAt(if (delay < 0) -delay else 0)
      val linesTo = list.drop(if (delay > 0) delay else 0)
      val after = list.takeRight(if (delay > 0) delay else 0)

      {
        before.map(r => Row.fromSeq(r.toSeq :+ null)) ++
          (linesFrom zip linesTo).par.map {
            case (f, t) => {
              val newVal = if (f.get(indexFrom) == null || t.get(indexFrom) == null || f.getDouble(indexFrom) == 0d || t.getDouble(indexFrom) == 0d) {
                0d
              } else {
                f.getDouble(indexFrom) - t.getDouble(indexFrom)
              }
              Row.fromSeq(f.toSeq :+ newVal)
            }
          } ++
          after.map(r => Row.fromSeq(r.toSeq :+ null))
      }.toIterator
    }

    val newSchema = StructType(df.schema.fields ++ Array(StructField(to, DoubleType, true)))
    df.repartition($"ISIN").mapPartitions(lines => part(lines))(RowEncoder.apply(newSchema))
  }
}
