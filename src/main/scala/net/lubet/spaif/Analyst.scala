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

// java -Xms12G -Xmx12G -XX:ReservedCodeCacheSize=128m -XX:MaxMetaspaceSize=256m -jar /usr/share/sbt/bin/sbt-launch.jar console
// java -Xms10G -Xmx10G -XX:ReservedCodeCacheSize=128m -XX:MaxMetaspaceSize=256m -jar "C:\Program Files (x86)\sbt\bin\sbt-launch.jar" console

object Analyst {

  import Context.spark.implicits._
  import Context.spark._
  import Context._

  def machinePrediction():Unit={
    
    val data = sql("""
    select d.* from data d
    where date>date_sub(now(),5)
    --join (
    --select isin, max(date) date from data group by isin
    --) as dm on d.isin=dm.isin and d.date=dm.date
    """)
    
    
    val pl = new Pipeline().
      setStages(Array(
        /*
        new StringIndexer().
          setInputCol("isin").
          setOutputCol("isin_i").
          setHandleInvalid("skip")
        ,
        new OneHotEncoderEstimator().
          setInputCols(Array("isin_i")). //, "ISIN_i")).
          setOutputCols(Array("isin_v")) //, "ISIN_v"))
        ,*/
        new VectorAssembler().
          setInputCols(colFeatures).
          setOutputCol("features")
      ))
    val preparedData = pl.fit(data).transform(data)
    
    //val rfc = RandomForestClassificationModel.load("spark/ml/rfc/gt2")
    //rfc.transform(preparedData)
    
    val models = Array("gt2","gt3","gt4","gt5").map(s =>
      RandomForestClassificationModel.load("spark/ml/rfc/"+s)
      )
    
    val res = models.foldLeft(preparedData)((acc,model)=> {
      model.transform(acc)
    })
    
    res.createOrReplaceTempView("prediction")
    
    sql(s"""select isin,date,${colPredicted.map(s=>s"`$s`").mkString(",")} from prediction order by date desc,${colPredicted.map(s=>s"`$s` desc").mkString(",")}""").show
    
  }

  def machineLearning(): Unit = {
    
    sqlContext.clearCache()
    
    val data_raw = sql("""
    select * from data 
    where isin in ("FR0000045072","FR0000130809","FR0000120172","FR0000054470","FR0000031122","FR0000120404")
    --where isin in (
    --select isin from quotation group by isin order by count(isin) desc limit 10
    --)
    """).cache()


    colLabels.foreach(buildModel(_,data_raw))
    
    /*buildModel("gt2",data_raw)
    
    buildModel("gt5",data_raw)
    
    buildModel("gt3",data_raw)
    buildModel("gt4",data_raw)*/

  }


  def buildModel(from:String, data_raw:DataFrame) = {
    val to = "prediction-"+from
    
    println(s"Build model for $from")
    
    val pertinentData=data_raw.select(from, colFeatures: _*).na.drop
    val Array(trainingData, testData) = pertinentData.randomSplit(Array(0.8, 0.2))

    val pl = new Pipeline().
      setStages(Array(
        new VectorAssembler().
          setInputCols(
            colFeatures
          ).
          setOutputCol("features")
      ))
      
    val rfc = new RandomForestClassifier().setNumTrees(50).
          setLabelCol(from).
          setFeaturesCol("features").
          setPredictionCol(to).
          setRawPredictionCol("rawPrediction-"+from).
          setProbabilityCol("probability-"+from)
          
    val model = rfc.fit(pl.fit(trainingData).transform(trainingData))
          
    val predictions = model.transform(pl.fit(testData).transform(testData))

    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol(from).
      setPredictionCol(to).
      setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

    predictions.groupBy(to,from).agg(count("*")).orderBy(desc(to)).show

    rfc.fit(pl.fit(pertinentData).transform(pertinentData)).write.overwrite().save(s"spark/ml/rfc/$from")
    
    val total = predictions.count
    val opportunitiesDetected = predictions.filter(col(to) === 1).count
    val opportunitiesReal = predictions.filter(col(from) === 1).count
    val falsePositive = predictions.filter(col(from) === 0 && col(to) === 1).count
    val falseNegative = predictions.filter(col(from) === 1 && col(to) === 0).count
    val ok = predictions.filter(col(from) === 1 && col(to) === 1).count
    val bad = predictions.filter($"P-S+5" < 0.01 && col(to) ===1).count
    
    val missed = opportunitiesReal - opportunitiesDetected
    
    val ratioDetection = 100 * opportunitiesDetected / opportunitiesReal
    val risk = 100 * falsePositive / opportunitiesDetected
    
    println(s"""Results for prediction on $from
    Total row:$total
    Opportunities Detected & Real:$opportunitiesDetected $opportunitiesReal (${100*opportunitiesDetected/opportunitiesReal}%)

    falsePositive:$falsePositive
    falseNegative:$falseNegative
    ok:$ok

    risk:$risk%
    bad decisions:$bad (${100 * bad/opportunitiesDetected}%)
    """)
  }
  
  val colFeatures = Array("D-L/XL-1",
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
          "XS/S")
          
    val colLabels = Array("gt2","gt3","gt4","gt5")
    def colPredicted = colLabels.map(s=>"prediction-"+s)
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
