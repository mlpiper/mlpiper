package org.apache.flink.streaming.scala.examples.common.ml

import org.apache.flink.api.common.JobExecutionResult

trait OnlineInferenceMapFunction[
Input,
Prediction,
Map <: PredictValidationCountersAndFunctions[Input, Prediction]] {
  protected var inferenceMapFunction: Map = _

  def fetchGlobalCounters(jobExecutionResult: JobExecutionResult): (Long, Long) = {
    inferenceMapFunction.fetchGlobalCounter(jobExecutionResult)
  }
}

object InferenceConstants {
  val UnableToPredictDoubleLabelReplacement: Double = -1.0
  val UnableToPredictStringLabelReplacement: String = "N/A"
}
