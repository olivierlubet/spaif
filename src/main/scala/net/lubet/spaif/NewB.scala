package net.lubet.spaif

import org.apache.spark.ml.classification._
import org.apache.spark.ml.linalg
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._

class NewB ( override val uid: String)
  extends ProbabilisticClassifier[org.apache.spark.ml.linalg.Vector,  NewB,  NewBModel] with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("nb"))

  override def copy(extra: ParamMap): NewB = ???

  override protected def train(dataset: Dataset[_]): NewBModel = ???
}

 class NewBModel ( override val uid: String)
   extends ProbabilisticClassificationModel[linalg.Vector, NewBModel] with MLWritable{

   def this() = this(Identifiable.randomUID("nb"))

   override protected def raw2probabilityInPlace(rawPrediction: linalg.Vector): linalg.Vector = ???

   override def write: MLWriter = ???

   override def numClasses: Int = ???

   override protected def predictRaw(features: linalg.Vector): linalg.Vector = ???

   override def copy(extra: ParamMap): NewBModel = ???

 }
