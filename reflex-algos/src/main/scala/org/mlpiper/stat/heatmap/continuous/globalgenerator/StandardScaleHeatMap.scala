package org.mlpiper.stat.heatmap.continuous.globalgenerator

import org.apache.flink.api.common.functions.RichMapFunction
import org.mlpiper.stat.heatmap.continuous.{GlobalParams, HeatMapValues}

/**
  * Class [[StandardScaleHeatMap]] will use parameters of globalParameters and transform input denseVector
  *
  * Functionality: x => x - x_mean / (2 * x_std)
  * Since, standard scaling will generate result most of (95%) whose outputs will be in range of [-1, 1],
  * we will first clip the results to be in the range [-1,1], 100% of the times.
  * then, the values will be brought down to [0, 1] by the following equation.
  *
  * x_final = (x + 1) / 2
  *
  * globalParameters' param1 will be mean and param2 will be standard deviation.
  */
class StandardScaleHeatMap
  extends RichMapFunction[(HeatMapValues, GlobalParams), HeatMapValues] {
  override def map(value: (HeatMapValues, GlobalParams)): HeatMapValues = {
    val meanVector = value._2.params1
    val stdVector = value._2.params2

    val keys = stdVector.keys

    val runningDivisor = keys.map(eachKey => (eachKey, {
      var div = 2 * stdVector(eachKey)

      /**
        * Replacing weights of zero to one to prevent "divide by zero" error condition.
        * If weight is zero then, we will simply return the actual weight without scaling it down.
        */
      if (div == 0) {
        div = 1
      }

      div
    })).toMap
    val runningSubtractor = meanVector

    val heatMapValues = value._1.heatMapValue

    val newHeatMapValues = heatMapValues.map(eachOldHeatValue => (eachOldHeatValue._1, {

      // Functionality: x => x - x_mean / (2 * x_std)
      var newHeatValue =
        (eachOldHeatValue._2 - runningSubtractor(eachOldHeatValue._1)) /
          runningDivisor(eachOldHeatValue._1)


      if (newHeatValue > 1) {
        newHeatValue = 1.0
      } else if (newHeatValue < -1) {
        newHeatValue = -1.0
      }

      // This ensures that all values are in the range [0,1] for the UI to display
      // x_final = (x + 1) / 2
      (newHeatValue + 1) / 2
    }))

    HeatMapValues(heatMapValue = newHeatMapValues, globalParams = Some(value._2))
  }
}
