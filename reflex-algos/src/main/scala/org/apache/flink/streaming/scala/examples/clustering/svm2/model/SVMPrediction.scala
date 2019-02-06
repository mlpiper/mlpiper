package org.apache.flink.streaming.scala.examples.clustering.svm2.model

import breeze.linalg.{DenseVector => BreezeDenseVector}
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.streaming.scala.examples.common.algorithm.PredictionOutput

import scala.collection.mutable

class SVMPrediction(predictedLabel: Option[Double],
                    dataVector: BreezeDenseVector[Double],
                    vectorTimestamp: Option[Long] = None,
                    val distance: Double)
  extends PredictionOutput(
    predictedLabel = predictedLabel,
    dataVector = dataVector,
    vectorTimestamp = vectorTimestamp) {

  def this(labeledVector: LabeledVector[Double],
           distance: Double) {
    this(
      if (labeledVector.hasLabel) Some(labeledVector.label) else None,
      labeledVector.vector,
      if (labeledVector.hasTimestamp) Some(labeledVector.timestamp) else None,
      distance)
  }


  override def toJson(): String = {
    val map = mutable.Map[String, Any](
      "data" -> vector.toArray.toList,
      "distance" -> distance
    )
    if (hasLabel) {
      map("label") = label
    }

    if (hasTimestamp) {
      map("timestamp") = timestamp
    }

    ParsingUtils.iterableToJSON(map)
  }
}
