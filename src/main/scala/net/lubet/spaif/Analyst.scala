package net.lubet.spaif


import org.apache.spark.ml._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//import net.lubet.spaif._
object Analyst {
  import Context.spark.implicits._

  def prepareData(): DataFrame = {

    val df = Database.stockValue
    //Serie.ema(df,)
    // ACA :
    val dfp = df.filter($"ISIN" === "FR0000045072").repartition($"ISIN").cache()
    println(dfp.count)
    dfp.show

    val df_withEMA = List(
      ("L", 0.15),
      ("M", 0.3),
      ("S", 0.65)
    ).foldLeft(dfp) { (acc, t) =>
      Serie.ema(acc, "Open", t._1, t._2)
    }.orderBy(asc("Date"))

    println(df_withEMA.count)
    df_withEMA.show

    val df_withR = Serie.rate(Serie.rate(
      Serie.rate(df_withEMA, "M", "L", "M/L"),
      "S", "M", "S/M"), //.orderBy(asc("Date")
      "S", "L", "S/L")
    println(df_withR.count)
    df_withR.show

    val df_withP = List(
      ("P-5", -5),
      ("P-15", -15),
      ("P-30", -30),
      ("P+5", 5),
      ("P+15", 15),
      ("P+30", 30)
    ).foldLeft(df_withR) { (acc, t) =>
      Serie.performance(acc, "S", t._1, t._2)
    }

    println(df_withP.count)
    df_withP.show

    val df_withPS = List(
      "S",
      "M",
      "L"
    ).foldLeft(df_withR) { (acc, t) =>
      Serie.performance(acc, t, "P" + t, -1)
    }


    def classification(df: DataFrame): DataFrame = {
      val newSchema = StructType(df.schema.fields ++ Array(StructField("classEMA", StringType, false)))
      val indexS = df.head().fieldIndex("S")
      val indexM = df.head().fieldIndex("M")
      val indexL = df.head().fieldIndex("L")

      Context.spark.createDataFrame(df.rdd.map { r: Row =>
        val s = r.getDouble(indexS)
        val m = r.getDouble(indexM)
        val l = r.getDouble(indexL)

        val newVal = if (s > m && m > l) "SML" else if (s > l && l > m) "SLM" else if (m > s && s > l) "MSL" else if (m > l && l > s) "MLS" else if (l > s && s > m) "LSM" else if (l > m && m > s) "LMS" else "==="

        Row.fromSeq(r.toSeq :+ newVal)
      }, newSchema)
    }

    val df_withC = classification(df_withP)

    val data_raw = df_withC

    df_withC.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("spark/stock_value.csv")

    df_withC.cache()
  }

  def machineLearning(data_raw:DataFrame): Unit = {

    val Array(trainingData, testData) = data_raw.na.drop.randomSplit(Array(0.8, 0.2))

    val pl = new Pipeline().
      setStages(Array(
        new StringIndexer().
          setInputCol("classEMA").
          setOutputCol("classEMA_i").
          setHandleInvalid("skip")
        ,
        /*new StringIndexer().
          setInputCol("ISIN").
          setOutputCol("ISIN_i").
          setHandleInvalid("skip")
        ,*/
        new OneHotEncoderEstimator().
          setInputCols(Array("classEMA_i")).//, "ISIN_i")).
          setOutputCols(Array("classEMA_v"))//, "ISIN_v"))
        ,
        new VectorAssembler().
          setInputCols(Array("classEMA_v",// "ISIN_v",
            //"Open", "Close", "High","Low","Number_of_Shares","Number_of_Trades","Turnover",
            "L", "M", "S",
            "M/L", "S/M", "S/L",
            "P-5",  "P-15",  "P-30"
          )).
          setOutputCol("features")
        ,
        new StandardScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")
        ,
        new VectorIndexer().
          setInputCol("scaledFeatures").
          setOutputCol("indexedFeatures").
          setMaxCategories(50).
          setHandleInvalid("skip")
        ,
        new LinearRegression().
          setMaxIter(100).
          setRegParam(0.3).
          setElasticNetParam(0.8).
          setLabelCol("P+5").
          setFeaturesCol("indexedFeatures")
      ))
    val model = pl.fit(trainingData)
    val predictions = model.transform(testData)

    predictions.select($"prediction", $"P+5", $"prediction" - $"P+5", $"features").show(50)

    // Select (prediction, true label) and compute test error.
    val lrEvaluator = new RegressionEvaluator().
      setLabelCol("P+5").
      setPredictionCol("prediction").
      setMetricName("rmse")
    val lrRmse = lrEvaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $lrRmse")


    val model_final = pl.fit(data_raw)
    model_final.write.overwrite().save("spark/analyse/model/linear-regression")
  }

}
