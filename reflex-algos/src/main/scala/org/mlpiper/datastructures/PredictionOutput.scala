package org.mlpiper.datastructures

import breeze.linalg.DenseVector

import scala.collection.mutable

/** Class is parent trait which will be parent to all Prediction. */
trait Prediction

case class PredictionOutput(predictedLabel: Option[Double],
                            dataVector: DenseVector[Double],
                            vectorTimestamp: Option[Long],
                            predictionType: PredictedOutputType.Type = PredictedOutputType.Classification)
  extends LabeledVector[Double](labelData = predictedLabel, data = dataVector, vectorTimestamp = vectorTimestamp)
    with Serializable with Prediction {

  def this(labelVector: LabeledVector[Double]) = {
    this(predictedLabel = if (labelVector.hasLabel) Some(labelVector.label) else None,
      dataVector = labelVector.vector,
      vectorTimestamp = if (labelVector.hasTimestamp) Some(labelVector.timestamp) else None)
  }

  def createMapOfObjectDescription(): mutable.Map[String, Any] = {
    val map = mutable.Map[String, Any](
      "data" -> vector.toArray.toList
    )
    if (hasLabel) {
      map("label") = label
    }

    if (hasTimestamp) {
      map("timestamp") = timestamp
    }

    map
  }
}

object PredictedOutputType extends Enumeration {
  type Type = Value

  val Classification = Value("classification")
}
