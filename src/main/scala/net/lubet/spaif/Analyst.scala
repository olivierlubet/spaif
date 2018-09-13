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

// java -Xms8G -Xmx8G -XX:ReservedCodeCacheSize=128m -XX:MaxMetaspaceSize=256m -jar /usr/share/sbt/bin/sbt-launch.jar console

object Analyst {

  import Context.spark.implicits._
  import Context.spark._
  import Context._

  def machinePrediction():Unit={
    val model = PipelineModel.load("spark/ml/RandomForestClassifier")
  }

  def machineLearning(): Unit = {

    val data_raw = sql("""
    select * from data where isin in ("FR0000045072","FR0000130809")
    """).cache()

    val Array(trainingData, testData) = data_raw.na.drop.randomSplit(Array(0.8, 0.2))

    val labelIndexer = new StringIndexer().
      setInputCol("Class").
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
        ,*/
        new StringIndexer().
          setInputCol("isin").
          setOutputCol("isin_i").
          setHandleInvalid("skip")
        ,
        new OneHotEncoderEstimator().
          setInputCols(Array("isin_i")). //, "ISIN_i")).
          setOutputCols(Array("isin_v")) //, "ISIN_v"))
        ,
        new VectorAssembler().
          setInputCols(Array(
          "isin_v",
          "D-L/XL-1",
          "D-M/L-1",
          "D-P-S-15-1",
          "D-P-S-30-1",
          "D-P-S-5-1",
          "D-S/L-1",
          "D-S/M-1",
          "D-XS/S-1",
          "L/XL",
          "M/L",
          "P-S-1",
          "P-S-1-P-S-5",
          "P-S-15",
          "P-S-15-P-S-30",
          "P-S-30",
          "P-S-5",
          "P-S-5-P-S-15",
          "S/L",
          "S/M",
          "XS/S"
          )).
          setOutputCol("features")
        ,
        new MinMaxScaler(). //StandardScaler()
          setInputCol("features").
          setOutputCol("scaledFeatures").
          setMin(0)
        ,
        new VectorIndexer().
          setInputCol("scaledFeatures").
          setOutputCol("indexedFeatures").
          setMaxCategories(50).
          setHandleInvalid("skip")
        ,
       // new LinearSVC().setMaxIter(10).setRegParam(0.1). // mouaif, toujours -1
          new RandomForestClassifier().setNumTrees(500).
        //new GBTClassifier().setFeatureSubsetStrategy("auto").
        //new NaiveBayes().setProbabilityCol("proba").
          setLabelCol("indexedLabel").
          setFeaturesCol("indexedFeatures")
        ,
        new IndexToString().
          setInputCol("prediction").
          setOutputCol("predictedLabel").
          setLabels(labelIndexer.labels)
      ))
    val predictions = pl.fit(trainingData).transform(testData)

    predictions.select($"predictedLabel", $"Class",  $"P-S+5", $"features").show(50)

    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("indexedLabel").
      setPredictionCol("prediction").
      setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

    predictions.groupBy("predictedLabel", "Class").agg(count("*")).orderBy($"predictedLabel").show


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
