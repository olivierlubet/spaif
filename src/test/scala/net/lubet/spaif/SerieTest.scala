package net.lubet.spaif

import java.io.File

import org.scalatest.FlatSpec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession



class SerieTest extends FlatSpec with SparkSessionWrapper {

  import spark.implicits._
  behavior of "SerieTest"

  val df = spark.createDataset(List(
    ("a", 1, 1d),
    ("a", 2, 1d),
    ("a", 3, 2d),
    ("a", 4, 3d),
    ("a", 5, 4d),
    ("b", 1, 10d),
    ("b", 2, 20d)
  )).toDF("ISIN", "id", "val").
    repartitionByRange($"ISIN").
    orderBy($"ISIN", $"id")

  df.show(10)

  it should "compute ema" in {
    Serie.ema(df, "val", "ema", 1).show
    val ema1 = Serie.ema(df, "val", "ema", 1).filter($"ISIN" === "a").collect()
    ema1.foreach {
      r: Row => assertResult(r.getDouble(3))(r.getDouble(2))
    }

    val ema2 = Serie.ema(df, "val", "ema", 0).filter($"ISIN" === "a").collect()
    ema2.foreach {
      r: Row => assertResult(r.getDouble(3))(ema2.head.getDouble(2))
    }

    val ema3: Array[Row] = Serie.ema(df, "val", "ema", 0.2).filter($"ISIN" === "b").collect()
    assertResult(ema3(1).getDouble(3))(ema3(0).getDouble(2) * 0.8 + ema3(1).getDouble(2) * 0.2)
  }

  it should "compute performance" in {
    Serie.performance(df,"val","p",0).show
    // Quand on compare à la valeur du jour, la performance est de 0%
    Serie.performance(df,"val","p",0).collect().foreach {r:Row =>
      assertResult(0d)(r.getDouble(3))
    }

    // Comparé à la valeur du lendemain (la série B ne doit donner qu'un unique élément = -0.5
    Serie.performance(df,"val","p",1).show
    val p1 = Serie.performance(df,"val","p",1).filter($"ISIN" === "b" && $"id"===1).collect()
    println(p1.mkString)
    assertResult(-0.5)(p1(0).getDouble(3))

    // Comparé à la valeur du jour d'avant (la série B ne doit donner qu'un unique élément = +1.0
    val p2 = Serie.performance(df,"val","p",-1).filter($"ISIN" === "b" && $"id"===2).collect()
    println(p2.mkString)
    assertResult(1d)(p2(0).getDouble(3))
  }
}
