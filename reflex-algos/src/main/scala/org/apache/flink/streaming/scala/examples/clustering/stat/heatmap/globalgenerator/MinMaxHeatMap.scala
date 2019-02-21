package org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.globalgenerator

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.{GlobalParams, HeatMapValues}

class MinMaxHeatMap
  extends RichMapFunction[(HeatMapValues, GlobalParams), HeatMapValues] {
  override def map(value: (HeatMapValues, GlobalParams)) = {
    val maxVector = value._2.params2
    val minVector = value._2.params1

    val keys = maxVector.keys

    val runningDivisor = keys.map(eachKey => (eachKey, {
      var diff = maxVector(eachKey) - minVector(eachKey)

      /**
        * Replacing weights of zero to one to prevent "divide by zero" error condition.
        * If weight is zero then, we will simply return the actual weight without scaling it down.
        */
      if (diff == 0) {
        diff = 1
      }

      diff
    })).toMap
    val runningSubtractor = minVector

    val heatMapValues = value._1.heatMapValue

    val newHeatMapValues = heatMapValues.map(eachOldHeatValue => (eachOldHeatValue._1, (eachOldHeatValue._2 - runningSubtractor(eachOldHeatValue._1)) / runningDivisor(eachOldHeatValue._1)))

    HeatMapValues(heatMapValue = newHeatMapValues, globalParams = Some(value._2))
  }
}
