package net.lubet.spaif


import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

trait LinearPrecisionReducerParams extends Params with HasInputCol with HasOutputCol {
  val precision: DoubleParam = new DoubleParam(this, "precision",
    "Precision of the targeted value")

  protected def validateAndTransformSchema(schema: StructType): StructType = {

    require(!schema.fieldNames.contains($(outputCol)),s"Output column ${$(outputCol)} already exists.")

    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != VectorType) {
      throw new Exception(s"Input type ${field.dataType} did not match VectorType")
    }

    schema.add(StructField($(outputCol),VectorType, false))
  }
}

class LinearPrecisionReducerModel (override val uid: String)   extends Model[LinearPrecisionReducerModel] with LinearPrecisionReducerParams {//} with MLWritable {

  override def copy(extra: ParamMap): LinearPrecisionReducerModel = {
    val copied = new LinearPrecisionReducerModel(uid)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val reScale = udf { vector: Vector =>
      Vectors.dense(for (value <- vector.toArray) yield {
        (value / $(precision)).round * $(precision)
      })
    }

    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}

class LinearPrecisionReducer(override val uid: String) extends Estimator[LinearPrecisionReducerModel] with LinearPrecisionReducerParams {//} with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("LinearPrecisionReducer"))

  setDefault(precision -> 0.01)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: Dataset[_]): LinearPrecisionReducerModel = {
    copyValues(new LinearPrecisionReducerModel(uid).setParent(this))
  }

  override def copy(extra: ParamMap): LinearPrecisionReducer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}