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

  def predict = {
    sqlContext.clearCache()
    val data = sql(
      """
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
      RandomForestClassificationModel.load("data/ml/rfc/" + s)
    )

    val res = models.foldLeft(preparedData)((acc, model) => {
      model.transform(acc)
    })

    res.createOrReplaceTempView("prediction") //.write.mode(SaveMode.Overwrite).partitionBy("isin").saveAsTable("prediction")

    println(sPrediction)
    sql(
      s"""select p.isin, symbol, name,date,
         |${colPredicted.map(s => s"`$s`").mkString(",")},
         |`probability-gt1`
         |from prediction p
         |left join stock s on p.isin=s.isin
         |order by date desc,
         |${colPredicted.map(s => s"`$s` desc").mkString(",")},
         |`probability-gt1` desc
       """.stripMargin).show(6)

    sql(
      s"""select p.isin, symbol, name,date,
         |`prediction-gt1`,`D-Close-1`
         |from prediction p
         |left join stock s on p.isin=s.isin
         |where `prediction-gt1`=1
         |order by date desc,`D-Close-1`
       """.stripMargin).show(6)

    println(sWatchlist)
    sql(
      s"""
        |select p.isin, symbol, name,date,
        |${colPredicted.map(s => s"`$s`").mkString(",")},
        |`probability-gt1`
        |from prediction p
        |left join stock s on p.isin=s.isin
        |join watchlist w on s.isin=w.isin
        |order by date desc,
        |${colPredicted.map(s => s"`$s` desc").mkString(",")},
        |`probability-gt1` desc
      """.stripMargin).show(100)
  }

  def show(isin:String)= {
    sql(
      s"""
        |select p.isin, symbol, name,date,Close,`D-Close-1` as DClose,
        |${colPredicted.map(s => s"`$s`").mkString(",")},
        |`probability-gt1`
        |from prediction p
        |left join stock s on p.isin=s.isin
        |where p.isin="${isin}"
        |order by date desc,
        |${colPredicted.map(s => s"`$s` desc").mkString(",")},
        |`probability-gt1` desc
      """.stripMargin).show
  }

  def learn: Unit = {

    sqlContext.clearCache()

    val data_raw = sql(
      """
    select * from data 
    --where isin in ("FR0000045072","FR0000130809","FR0000120172","FR0000054470","FR0000031122","FR0000120404")
    --where isin in (
    --select isin from quotation group by isin order by count(isin) desc limit 20
    --)
    """).cache()

    colLabels.foreach(buildModel(_, data_raw))
  }

  def buildModel(from: String, data_raw: DataFrame) = {
    val to = "prediction-" + from

    println(s"Build model for $from")

    val pertinentData = data_raw.select(from, colFeatures: _*).na.drop
    val Array(trainingData, testData) = pertinentData.randomSplit(Array(0.08, 0.02))

    val pl = new Pipeline().
      setStages(Array(
        new VectorAssembler().
          setInputCols(
            colFeatures
          ).
          setOutputCol("features"),
        new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")//,
        //new LinearPrecisionReducer().setInputCol("scaledFeatures").setOutputCol("reducedFeatures")
      ))

    val rfc = new RandomForestClassifier().setNumTrees(50).
    setLabelCol(from).
      setFeaturesCol("features").
      setPredictionCol(to).
      setRawPredictionCol("rawPrediction-" + from).
      setProbabilityCol("probability-" + from)

    val model = rfc.fit(pl.fit(trainingData).transform(trainingData))

/*
    val nb = new NaiveBayes().
      setSmoothing(0.9).
      setLabelCol(from).
      setFeaturesCol("reducedFeatures").
      setPredictionCol(to).
      setRawPredictionCol("rawPrediction-" + from).
      setProbabilityCol("probability-" + from)

    val model = nb.fit(pl.fit(pertinentData).transform(trainingData))
*/
    val predictions = model.transform(pl.fit(testData).transform(testData))

    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol(from).
      setPredictionCol(to).
      setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

    predictions.groupBy(to, from).agg(count("*")).orderBy(desc(to)).show

    //rfc.fit(pl.fit(pertinentData).transform(pertinentData)).write.overwrite().save(s"data/ml/rfc/$from")

    lazy val total = predictions.count
    lazy val opportunitiesDetected = predictions.filter(col(to) === 1).count
    lazy val opportunitiesReal = predictions.filter(col(from) === 1).count
    lazy val falsePositive = predictions.filter(col(from) === 0 && col(to) === 1).count
    lazy val falseNegative = predictions.filter(col(from) === 1 && col(to) === 0).count
    lazy val ok = predictions.filter(col(from) === 1 && col(to) === 1).count
    lazy val bad = predictions.filter($"P-XS+5" < 0.005 && col(to) === 1).count
    lazy val missed = opportunitiesReal - opportunitiesDetected
    lazy val risk = 100 * falsePositive / opportunitiesDetected

    println(
      s"""Results for prediction on $from
    Total row:$total
    Opportunities Detected & Real:$opportunitiesDetected $opportunitiesReal (${100 * opportunitiesDetected / opportunitiesReal}%)

    risk (false positive / opportunities detected):$risk%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): ${100 * bad / opportunitiesDetected}%
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

  val colLabels = Array("gt1", "gt2", "gt3", "gt4", "gt5", "gt6")
  lazy val colPredicted = colLabels.map(s => "prediction-" + s)

  lazy val sPrediction =
    """
      | ____________ ___________ _____ _____ _____ _____ _____ _   _  _____
      | | ___ \ ___ \  ___|  _  \_   _/  __ \_   _|_   _|  _  | \ | |/  ___|
      | | |_/ / |_/ / |__ | | | | | | | /  \/ | |   | | | | | |  \| |\ `--.
      | |  __/|    /|  __|| | | | | | | |     | |   | | | | | | . ` | `--. \
      | | |   | |\ \| |___| |/ / _| |_| \__/\ | |  _| |_\ \_/ / |\  |/\__/ /
      | \_|   \_| \_\____/|___/  \___/ \____/ \_/  \___/ \___/\_| \_/\____/
      |
      |
    """.stripMargin

  lazy val sWatchlist =
    """
      |  _    _  ___ _____ _____  _   _   _     _____ _____ _____
      | | |  | |/ _ \_   _/  __ \| | | | | |   |_   _/  ___|_   _|
      | | |  | / /_\ \| | | /  \/| |_| | | |     | | \ `--.  | |
      | | |/\| |  _  || | | |    |  _  | | |     | |  `--. \ | |
      | \  /\  / | | || | | \__/\| | | | | |_____| |_/\__/ / | |
      |  \/  \/\_| |_/\_/  \____/\_| |_/ \_____/\___/\____/  \_/
      |
      |
    """.stripMargin
}

/*

Random Forest Classifier
Build model for gt1
Test Error = 0.36573729094220897
+--------------+---+--------+
|prediction-gt1|gt1|count(1)|
+--------------+---+--------+
|           1.0|1.0|    7723|
|           1.0|0.0|    5479|
|           0.0|1.0|   29860|
|           0.0|0.0|   53562|
+--------------+---+--------+

Results for prediction on gt1
    Total row:96624
    Opportunities Detected & Real:13202 37583 (35%)

    risk (false positive / opportunities detected):41%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): 36%

Build model for gt2
Test Error = 0.2704249639024796
+--------------+---+--------+
|prediction-gt2|gt2|count(1)|
+--------------+---+--------+
|           1.0|0.0|    1138|
|           1.0|1.0|    1669|
|           0.0|1.0|   24895|
|           0.0|0.0|   68565|
+--------------+---+--------+

Results for prediction on gt2
    Total row:96267
    Opportunities Detected & Real:2807 26564 (10%)

    risk (false positive / opportunities detected):40%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): 29%

Build model for gt3
Test Error = 0.19346159439862043
+--------------+---+--------+
|prediction-gt3|gt3|count(1)|
+--------------+---+--------+
|           1.0|0.0|     266|
|           1.0|1.0|     403|
|           0.0|1.0|   18357|
|           0.0|0.0|   77236|
+--------------+---+--------+

Results for prediction on gt3
    Total row:96262
    Opportunities Detected & Real:669 18760 (3%)

    risk (false positive / opportunities detected):39%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): 26%

Build model for gt4
Test Error = 0.27900079844872816
+--------------+---+--------+
|prediction-gt4|gt4|count(1)|
+--------------+---+--------+
|           1.0|1.0|     197|
|           1.0|0.0|     150|
|           0.0|1.0|   26756|
|           0.0|0.0|   69334|
+--------------+---+--------+

Results for prediction on gt4
    Total row:96437
    Opportunities Detected & Real:347 26953 (1%)

    risk (false positive / opportunities detected):43%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): 26%

Build model for gt5
Test Error = 0.2342585200468641
+--------------+---+--------+
|prediction-gt5|gt5|count(1)|
+--------------+---+--------+
|           1.0|1.0|      12|
|           1.0|0.0|       9|
|           0.0|1.0|   22585|
|           0.0|0.0|   73843|
+--------------+---+--------+

Results for prediction on gt5
    Total row:96449
    Opportunities Detected & Real:21 22597 (0%)

    risk (false positive / opportunities detected):42%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): 14%

Build model for gt6
Test Error = 0.18942808882809503
+--------------+---+--------+
|prediction-gt6|gt6|count(1)|
+--------------+---+--------+
|           1.0|0.0|       3|
|           0.0|1.0|   18277|
|           0.0|0.0|   78221|
+--------------+---+--------+

Results for prediction on gt6
    Total row:96501
    Opportunities Detected & Real:3 18277 (0%)

    risk (false positive / opportunities detected):100%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): 66%

 */