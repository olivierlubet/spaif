package net.lubet.spaif

//import net.lubet.spaif._

import net.lubet.spaif.Analyst.{colFeatures, pipelineModel}
import net.lubet.spaif.Indicators.{pivot, time}
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.classification._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}

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
    select d.*
    from data d
    where date>date_sub(now(),300)
    """)

    val preparedData = pipelineModel(Database.data.na.drop).transform(data)

    val models = colLabels.map(s =>
      RandomForestClassificationModel.load("data/ml/rfc/" + s)
    )

    val resRFC = models.foldLeft(preparedData)((acc, model) => {
      model.transform(acc)
    })

    val resK = KMeansModel.load(s"data/ml/kmeans").transform(resRFC)

    resK.write.mode(SaveMode.Overwrite).saveAsTable("prediction") //.write.mode(SaveMode.Overwrite).partitionBy("isin").saveAsTable("prediction")

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
         |`prediction-gt1`,`P-Close-1` as PClose
         |from prediction p
         |left join stock s on p.isin=s.isin
         |where `prediction-gt1`=1
         |order by date desc,`P-Close-1`
       """.stripMargin).show(6)

    println(sWatchlist)
    sql(
      s"""
         |select p.isin, symbol, name,date, kmeans ,
         |${colPredicted.map(s => s"`$s`").mkString(",")},
         |`probability-gt1`
         |from prediction p
         |left join stock s on p.isin=s.isin
         |join watchlist w on s.isin=w.isin
         |order by date desc,
         |${colPredicted.map(s => s"`$s` desc").mkString(",")},
         |`probability-gt1` desc
      """.stripMargin).limit(Database.watchlist.count().toInt * 2).show(100)
  }

  def show(isin: String) = {
    sql(
      s"""
         |select p.isin, symbol, name,date,Close,`P-Close-1` as PClose, kmeans,
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

  def giveMe(cluster: Integer) = {
    sql(
      s"""
         |select p.isin, symbol, name,date,Close,`P-Close-1` as PClose, kmeans
         |from prediction p
         |left join stock s on p.isin=s.isin
         |where kmeans=${cluster}
         |order by date desc
      """.stripMargin).show
  }

  def learn: Unit = {

    sqlContext.clearCache()

    colLabels.foreach(buildModel(_, Database.data))

    buildCluster(Database.data)
  }

  def buildModel(from: String, data_raw: DataFrame) = {
    val to = "prediction-" + from

    println(s"Build model for $from")

    val pertinentData = data_raw.select(from, colFeatures: _*).na.drop

    val fitedPipeline = pipelineModel(pertinentData)

    val rfc = new RandomForestClassifier().setNumTrees(50).
      setLabelCol(from).
      setFeaturesCol("features").
      setPredictionCol(to).
      setRawPredictionCol("rawPrediction-" + from).
      setProbabilityCol("probability-" + from)

    rfc.fit(fitedPipeline.transform(pertinentData)).write.overwrite().save(s"data/ml/rfc/$from")

  }

  def evaluate(from: String, data_raw: DataFrame) = {
    val to = "prediction-" + from

    val rfc = new RandomForestClassifier().setNumTrees(50).
      setLabelCol(from).
      setFeaturesCol("features").
      setPredictionCol(to).
      setRawPredictionCol("rawPrediction-" + from).
      setProbabilityCol("probability-" + from)

    val pertinentData = data_raw.select(from, colFeatures: _*).na.drop
    val Array(trainingData, testData) = pertinentData.randomSplit(Array(0.08, 0.02))

    val fitedPipeline = pipelineModel(pertinentData)

    val model = rfc.fit(fitedPipeline.transform(trainingData))

    val predictions = model.transform(fitedPipeline.transform(testData))

    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol(from).
      setPredictionCol(to).
      setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

    predictions.groupBy(to, from).agg(count("*")).orderBy(desc(to)).show

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
    Opportunities Detected & Real:$opportunitiesDetected $opportunitiesReal (${100 * opportunitiesDetected / opportunitiesReal}%)

    risk (false positive / opportunities detected):$risk%
    bad decisions (predicted positive but in fact < 0.005 / opportunities detected): ${100 * bad / opportunitiesDetected}%
    """)
  }

  def buildCluster(data_raw: DataFrame) = {
    val pertinentData = data_raw.select(colFeatures.head, colFeatures.tail: _*).na.drop

    val fitedPipeline = pipelineModel(pertinentData)

    new KMeans().
      setK(15).setSeed(1L).setMaxIter(100).
      setFeaturesCol("scaledFeatures").
      setPredictionCol("kmeans").
      fit(fitedPipeline.transform(pertinentData)).
      write.overwrite().save(s"data/ml/kmeans")

  }

  def elbowWSSSE(data_raw: DataFrame) = {
    val pertinentData = data_raw.select(colFeatures.head, colFeatures.tail: _*).na.drop

    val fitedPipeline = pipelineModel(pertinentData)
    val data = fitedPipeline.transform(pertinentData).cache()

    for (k <- 3 until 90 by 3) {
      val wssse = new KMeans().
        setK(k).setSeed(1L).setMaxIter(100).
        setFeaturesCol("scaledFeatures").
        setPredictionCol("kmeans").
        fit(data).
        computeCost(data)

      println(s"$k;$wssse")
    }
  }

  def pipelineModel(pertinentData: DataFrame): PipelineModel = {
    val pl = new Pipeline().
      setStages(Array(
        new VectorAssembler().
          setInputCols(
            colFeatures
          ).
          setOutputCol("features"),
        new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
      ))
    pl.fit(pertinentData.na.drop())
  }

  //data.schema.foreach(s=>println(s""""${s.name}","""))
  val colFeatures = Array(
    "Close/BOL-H-Close",
    "Close/BOL-L-Close",
    "D-L/XL-1",
    "D-M/L-1",
    "D-P-XS-15-1",
    "D-P-XS-30-1",
    "D-P-XS-5-1",
    "D-S/L-1",
    "D-S/M-1",
    "D-XS/L-1",
    "D-XS/S-1",
    "D-XS/XS-H-1",
    "D-XS/XS-L-1",
    "D-XS/XS-O-1",
    "L/XL",
    "M/BOL-H-M",
    "M/BOL-L-M",
    "M/L",
    //"P-Close+1",
    //"P-Close+5",
    "P-Close-1",
    //"P-XS+15",
    //"P-XS+30",
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
    "XL/XXL",
    "XS/BOL-H-XS",
    "XS/BOL-L-XS",
    "XS/L",
    "XS/S",
    "XS/XS-H",
    "XS/XS-L",
    "XS/XS-O"
  )

  val colLabels = Array("gt1", "gt2", "gt3")
  //, "gt4")
  //, "gt5")//, "gt6"
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