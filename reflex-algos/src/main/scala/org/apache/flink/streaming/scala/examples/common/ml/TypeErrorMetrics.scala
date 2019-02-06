package org.apache.flink.streaming.scala.examples.common.ml

import breeze.linalg.max
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector

object TypeErrors extends Enumeration {
  val TRUE_POSITIVE: String = "TP"
  val TRUE_NEGATIVE: String = "TN"
  val FALSE_POSITIVE: String = "FP"
  val FALSE_NEGATIVE: String = "FN"
}

trait TypeErrorMetrics {

  /*

      Prediction ->
    T
    r
    u
    t
    h
    v
        F         T
   F  0, 0       0, 1
   T  1, 0       1, 1
   */

  /* this should be moved and generalized in later versions */
  private   val confusionMatrix = Array(Array(0.0, 0.0), Array(0.0, 0.0))
  protected var accumulatedPrecisionScore: Double = 0.0
  protected var accumulatedRecallScore: Double = 0.0
  protected var accumulatedF1Score: Double = 0.0

  def getPrecision() : Double = {
    accumulatedPrecisionScore
  }

  def getRecall() : Double = {
    accumulatedRecallScore
  }

  def getF1Score() : Double = {
    accumulatedF1Score
  }

  def recordTypeError(truth: Double, pred: Double) : Unit = {
    recordTypeErrorEntry(truth, pred)
    val metricsSet = getTypeErrorMetrics()
    accumulatedPrecisionScore = metricsSet.precision
    accumulatedRecallScore = metricsSet.recall
    accumulatedF1Score = metricsSet.f1Score
  }

  def recordTypeErrorEntry(truth: Double, pred: Double): Unit = {
    assert((truth == -1.0 || truth == 0.0 || truth == 1.0) &&
      (pred == -1.0 || pred == 0.0 || pred == 1.0))
    confusionMatrix(max(truth.toInt, 0))(max(pred.toInt, 0)) += 1
  }

  def getTypeErrorMetrics() : TypeErrorMetricSet = {
    // update precision, recall, and f1-score
    val tp: Double = confusionMatrix(1)(1)
    val fp: Double = confusionMatrix(0)(1)
    val tn: Double = confusionMatrix(1)(0)
    val fn: Double = confusionMatrix(0)(0)
    val predPos: Double = tp + fp
    val truePos: Double = tp + fn

    val precision = if (predPos == 0.0 || tp == 0.0) 1.0 else tp / (tp + fp)
    val recall = if (truePos == 0.0 || tp == 0.0) 1.0 else tp / (tp + fn)
    val f1Score = if (precision + recall == 0.0 || tp == 0.0) 1.0 else 2 * (precision * recall / (precision + recall))

    TypeErrorMetricSet(precision, recall, f1Score)
  }
}

