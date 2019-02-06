package org.apache.flink.streaming.scala.examples.common.ml

import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.scala.examples.common.parameters.ml.{Validation, ValidationAccumulators}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterMap
import org.apache.flink.streaming.scala.examples.common.stats._


trait PredictValidationCountersAndFunctions[Input, Prediction]
  extends PredictFunction[Input, Prediction]{
  var localValidPredictionCount: Long = 0
  var localPredictionCount: Long = 0
  var intervalValidPredictionCount: Long = 0

  var validPredictionCounter: GlobalAccumulator[Long] = _
  var totalPredictionCounter: GlobalAccumulator[Long] = _

  val params: ArgumentParameterMap

  //[REF-287] TODO use the notion of a reflex component id to make these strings unique
  object GlobalCounters {
    val TOTAL_PREDICTIONS_ACCUMULATOR_NAME = "total-predictions"
    val CORRECT_PREDICTIONS_ACCUMULATOR_NAME = "correct-predictions"
  }

  require(params.get(Validation).isDefined, Validation.errorMessage)
  protected val enabledValidation: Boolean = params.get(Validation).get
  protected val enableAccumulators: Boolean = params.get(ValidationAccumulators).get

  def initGlobalCounter(enabledValidation: Boolean, runtimeContext: RuntimeContext): Unit = {
    if (enabledValidation && enableAccumulators) {

      if (validPredictionCounter == null) {
        validPredictionCounter =
          StatInfo(name = GlobalCounters.CORRECT_PREDICTIONS_ACCUMULATOR_NAME,
            StatPolicy.REPLACE,
            StatPolicy.SUM)
            .toGlobalStat(localValidPredictionCount,
              accumDataType = AccumData.getGraphType(localValidPredictionCount),
              infoType = InfoType.General)
      }
      if (totalPredictionCounter == null) {
        totalPredictionCounter =
          StatInfo(name = GlobalCounters.TOTAL_PREDICTIONS_ACCUMULATOR_NAME,
            StatPolicy.REPLACE,
            StatPolicy.SUM)
            .toGlobalStat(localPredictionCount,
              accumDataType = AccumData.getGraphType(localPredictionCount),
              infoType = InfoType.General)
      }
      
      validPredictionCounter.updateFlinkAccumulator(runtimeContext)
      totalPredictionCounter.updateFlinkAccumulator(runtimeContext)
    }
  }

  def fetchGlobalCounter(jobExecutionResult: JobExecutionResult)
  : (Long, Long) = {

    if (enabledValidation && enabledValidation) {
      (jobExecutionResult
        .getAccumulatorResult[AccumulatorInfo[Long]](GlobalCounters.TOTAL_PREDICTIONS_ACCUMULATOR_NAME).value,
        jobExecutionResult
          .getAccumulatorResult[AccumulatorInfo[Long]](GlobalCounters.CORRECT_PREDICTIONS_ACCUMULATOR_NAME).value)
    } else {
      (0L, 0L)
    }
  }

  def closeGlobalCounter(enabledValidation: Boolean, runtimeContext: RuntimeContext): Unit = {
    if (enabledValidation && enableAccumulators) {
      validPredictionCounter.localUpdate(localValidPredictionCount)
      totalPredictionCounter.localUpdate(localPredictionCount)

      validPredictionCounter.updateFlinkAccumulator(runtimeContext)
      totalPredictionCounter.updateFlinkAccumulator(runtimeContext)
    }
  }

  final def resetIntervalCounts() : Unit = {
    intervalValidPredictionCount = 0
  }
  final def getIntervalValidCount() : Long = {
    intervalValidPredictionCount
  }

  protected def validPrediction(input: Input, prediction: Prediction): Boolean

  /**
    * Increments [[localValidPredictionCount]] if [[validPrediction]] returns true.
    * @param input Input
    * @param prediction Prediction
    * @return Prediction
    */
  protected def validateInput(input: Input, prediction: Prediction): Prediction = {
    localPredictionCount += 1
    if (validPrediction(input, prediction)) {
      localValidPredictionCount += 1
      intervalValidPredictionCount += 1
    }
    prediction
  }
}