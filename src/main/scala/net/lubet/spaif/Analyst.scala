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

  def predict={
    sqlContext.clearCache()
    val data = sql("""
    select d.* from data d
    where date>date_sub(now(),10)
    --join (
    --select isin, max(date) date from data group by isin
    --) as dm on d.isin=dm.isin and d.date=dm.date
    """)

    val pl = new Pipeline().
      setStages(Array(
        new VectorAssembler().
          setInputCols(colFeatures).
          setOutputCol("features")
      ))
    val preparedData = pl.fit(data).transform(data)

    val models = colLabels.map(s =>
      RandomForestClassificationModel.load("data/ml/rfc/"+s)
      )
    
    val res = models.foldLeft(preparedData)((acc,model)=> {
      model.transform(acc)
    })

    res.createOrReplaceTempView("prediction")//.write.mode(SaveMode.Overwrite).partitionBy("isin").saveAsTable("prediction")
    sql(
      s"""select isin,date,
         |${colPredicted.map(s=>s"`$s`").mkString(",")},
         |`probability-gt3`
         |from prediction order by date desc,
         |${colPredicted.map(s=>s"`$s` desc").mkString(",")},
         |`probability-gt3` desc
       """.stripMargin).show
  }

  def learn: Unit = {
    
    sqlContext.clearCache()
    
    val data_raw = sql("""
    select * from data 
    --where isin in ("FR0000045072","FR0000130809","FR0000120172","FR0000054470","FR0000031122","FR0000120404")
    --where isin in (
    --select isin from quotation group by isin order by count(isin) desc limit 20
    --)
    """).cache()

    colLabels.foreach(buildModel(_,data_raw))
  }

  def buildModel(from:String, data_raw:DataFrame) = {
    val to = "prediction-"+from
    
    println(s"Build model for $from")
    
    val pertinentData=data_raw.select(from, colFeatures: _*).na.drop
    val Array(trainingData, testData) = pertinentData.randomSplit(Array(0.08, 0.02))

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

    rfc.fit(pl.fit(pertinentData).transform(pertinentData)).write.overwrite().save(s"data/ml/rfc/$from")
    
    lazy val total = predictions.count
    lazy val opportunitiesDetected = predictions.filter(col(to) === 1).count
    lazy val opportunitiesReal = predictions.filter(col(from) === 1).count
    lazy val falsePositive = predictions.filter(col(from) === 0 && col(to) === 1).count
    lazy val falseNegative = predictions.filter(col(from) === 1 && col(to) === 0).count
    lazy val ok = predictions.filter(col(from) === 1 && col(to) === 1).count
    lazy val bad = predictions.filter($"P-XS+5" < 0.005 && col(to) ===1).count
    lazy val missed = opportunitiesReal - opportunitiesDetected
    lazy val risk = 100 * falsePositive / opportunitiesDetected
    
    println(s"""Results for prediction on $from
    Total row:$total
    Opportunities Detected & Real:$opportunitiesDetected $opportunitiesReal (${100*opportunitiesDetected/opportunitiesReal}%)

    risk (false positive / opportunities detected):$risk%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): ${100 * bad/opportunitiesDetected}%
    """)
  }

  val colFeatures = Array(
    "D-L/XL-1",
    "D-M/L-1",
    "D-P-XS-15-1",
    "D-P-XS-30-1",
    "D-P-XS-5-1",
    "D-S/L-1",
    "D-S/M-1",
    "D-XS/L-1",
    "D-XS/S-1",
    "L/XL",
    "M/L",
    //"P-XS+15",
    //"P-XS+5",
    "P-XS-1",
    "P-XS-1-P-XS-5",
    "P-XS-15",
    "P-XS-15-P-XS-30",
    "P-XS-30",
    "P-XS-5",
    "P-XS-5-P-XS-15",
    "S/L",
    "S/M",
    "XS/L",
    "XS/S")
          
    val colLabels = Array("gt1","gt2","gt3","gt4","gt5","gt6")
    def colPredicted = colLabels.map(s=>"prediction-"+s)
}
