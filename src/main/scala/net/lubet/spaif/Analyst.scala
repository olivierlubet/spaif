package net.lubet.spaif

//import net.lubet.spaif._

import org.apache.spark.ml._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.classification._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs._


object Analyst {

  import Context.spark.implicits._
  import Context.spark._
  import Context._

  def prepareData(): DataFrame = {

    val df = Database.quotation
    //Serie.ema(df,)
    // ACA :
    val dfp = df.filter($"ISIN" === "FR0000045072").repartition($"ISIN").cache()
    //println(dfp.count)
    //dfp.show

    println("df_withEMA")
    val df_withEMA = List(
      ("XL", 0.01),
      ("L", 0.03),
      ("M", 0.1),
      ("S", 0.3),
      ("XS", 0.65)
    ).foldLeft(dfp) { (acc, t) =>
      Serie.ema(acc, "Open", t._1, t._2)
    }.orderBy(asc("Date"))


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

    println("df_withC")
    val df_withC = classification(df_withEMA)
    //println(df_withEMA.count)
    //df_withEMA.show

    println("df_withR")
    val df_withR = Serie.rate(Serie.rate(
      Serie.rate(df_withC, "M", "L", "M/L"),
      "S", "M", "S/M"), //.orderBy(asc("Date")
      "S", "L", "S/L")
    //println(df_withR.count)
    //df_withR.show

    println("df_withP")
    val df_withP = List(
      ("P-1", -1),
      ("P-5", -5),
      ("P-15", -15),
      ("P-30", -30),
      ("P+5", 5),
      ("P+15", 15)
    ).foldLeft(df_withR) { (acc, t) =>
      println("df_withP " + t._1)
      Serie.performance(acc, "S", t._1, t._2)
    }

    println("df_withEMA++")
    val df_withEMA2 = List(
      "P-5",
      "P-15",
      "S/M",
      "M/L",
      "S/L"
    ).foldLeft(df_withP) { (acc, t) =>
      Serie.ema(acc, t, "EMA-" + t, 0.65)
    }.orderBy(asc("Date"))

    println("df_withD")
    val df_withD = List(
      "EMA-S/M",
      "EMA-M/L",
      "EMA-S/L",
      "EMA-P-5",
      "EMA-P-15"
    ).foldLeft(df_withEMA2) { (acc, t) =>
      println("df_withD " + t)
      Serie.derivative(acc, t, "D" + t, -1)
    }

    //println(df_withPPP.count)
    //df_withP.show

    val df_withT = df_withD.withColumn("targetInt", ($"P+5" * 100).cast("int")).
      withColumn("targetClass",
        when($"targetInt" < -1, "Negative").
          when($"targetInt" < -2, "Neutral").
          when($"targetInt" < 5, "Positive").
          otherwise("Great !!!")).
    withColumn("binTargetClass",
        when($"targetInt" < 5, "Bof").
        otherwise("Great !!!"))

    val data_raw = df_withT.cache()

    println("Write")
    data_raw.write.mode("overwrite").saveAsTable("data")
    data_raw.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("spark/stock_value.csv")

    val fs = FileSystem.get(Context.spark.sparkContext.hadoopConfiguration)
    val filePath = fs.globStatus(new Path("spark/stock_value.csv/part*"))(0).getPath()
    fs.rename(filePath, new Path("data.csv"))

    data_raw
  }

  def machineLearning(data_raw: DataFrame): Unit = {

    val Array(trainingData, testData) = data_raw.na.drop.randomSplit(Array(0.8, 0.2))

    val labelIndexer = new StringIndexer().
      setInputCol("binTargetClass").
      setOutputCol("indexedLabel").
      fit(data_raw)

    val pl = new Pipeline().
      setStages(Array(

        labelIndexer,
        /*
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
          setInputCols(Array("classEMA_i")). //, "ISIN_i")).
          setOutputCols(Array("classEMA_v")) //, "ISIN_v"))
        ,*/
        new VectorAssembler().
          setInputCols(Array(//"classEMA_v", // "ISIN_v",
            //"Open", "Close", "High","Low","Number_of_Shares","Number_of_Trades","Turnover",
            //"L", "M", "S",
            "EMA-S/L", "EMA-S/M", "EMA-M/L",
            "EMA-P-5", "EMA-P-15",
            "DEMA-S/M", "DEMA-M/L", "DEMA-S/L", "DEMA-P-5", "DEMA-P-15",
            "P-1", "P-5", "P-15", "P-30"
            //"M/L", "S/M", "S/L",
          )).
          setOutputCol("features")
        ,
        /*new MinMaxScaler() //StandardScaler()
          setInputCol("features")
          setOutputCol("scaledFeatures")
          setMin(0)
        ,
        new VectorIndexer().
          setInputCol("scaledFeatures").
          setOutputCol("indexedFeatures").
          setMaxCategories(50).
          setHandleInvalid("skip")
        ,*/
       // new LinearSVC().setMaxIter(10).setRegParam(0.1).
          new RandomForestClassifier().//.setNumTrees(50).
        //new GBTClassifier().setFeatureSubsetStrategy("auto").
          setLabelCol("indexedLabel").
          setFeaturesCol("features")
        ,
        new IndexToString()
          .setInputCol("prediction")
          .setOutputCol("predictedLabel")
          .setLabels(labelIndexer.labels)
      ))
    val predictions = pl.fit(trainingData).transform(testData)

    predictions.select($"predictedLabel", $"targetClass",  $"P+5", $"features").show(50)

    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("indexedLabel").
      setPredictionCol("prediction").
      setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

    predictions.groupBy("predictedLabel", "targetClass").agg(count("*")).orderBy($"predictedLabel").show


    pl.fit( data_raw.na.drop).write.overwrite().save("spark/ml/RandomForestClassifier")
  }

  /*
new GBTClassifier().setFeatureSubsetStrategy("auto").
accuracy: Double = 0.894431554524362
+--------------+-----------+--------+
|predictedLabel|targetClass|count(1)|
+--------------+-----------+--------+
|           Bof|   Positive|     553|
|           Bof|  Great !!!|      73|
|           Bof|   Negative|     206|
|     Great !!!|   Positive|      11|
|     Great !!!|  Great !!!|      12|
|     Great !!!|   Negative|       7|
+--------------+-----------+--------+

  RandomForestClassifier
  accuracy = 0.6733966745843231
+--------------+-----------+--------+
|predictedLabel|targetClass|count(1)|
+--------------+-----------+--------+
|     Great !!!|  Great !!!|       7|
|     Great !!!|   Negative|       4|
|     Great !!!|   Positive|      12|
|      Negative|  Great !!!|       6|
|      Negative|   Negative|      60|
|      Negative|   Positive|      29|
|      Positive|   Positive|     500|
|      Positive|  Great !!!|      74|
|      Positive|   Negative|     150|
+--------------+-----------+--------+

new RandomForestClassifier().setNumTrees(50).
accuracy: Double = 0.9025522041763341
+--------------+-----------+--------+
|predictedLabel|targetClass|count(1)|
+--------------+-----------+--------+
|           Bof|   Positive|     562|
|           Bof|  Great !!!|      82|
|           Bof|   Negative|     213|
|     Great !!!|   Positive|       2|
|     Great !!!|  Great !!!|       3|
+--------------+-----------+--------+

new LinearSVC().setMaxIter(10).setRegParam(0.1).
+--------------+-----------+--------+
|predictedLabel|targetClass|count(1)|
+--------------+-----------+--------+
|           Bof|  Great !!!|      83|
|           Bof|   Positive|     544|
|           Bof|   Negative|     210|
+--------------+-----------+--------+
   */

}
