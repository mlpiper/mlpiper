package com.parallelmachines.reflex.components.spark.batch.algorithms

trait SparkBatchInferenceMap[Input, Prediction] extends Serializable {
  def predict(input: Input, enableValidationPerMap: Boolean): (Prediction, Boolean)

  def validPrediction(input: Input, prediction: String): Boolean

  def setLabelField(fieldName: String)
}
