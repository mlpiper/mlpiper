package org.mlpiper.stat.heatmap.continuous

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator

class AccumulatorUpdater extends RichMapFunction[HeatMapValues, HeatMapValues] {
  private var globalStat: GlobalAccumulator[HeatMapValues] = _

  def updateStatAccumulator(heatMapValues: HeatMapValues): Unit = {
    if (globalStat != null) {
      globalStat.localUpdate(heatMapValues)
    } else {
      globalStat = HeatMap.getAccumulator(heatMapValues)
    }
    globalStat.updateFlinkAccumulator(this.getRuntimeContext)
  }

  override def map(value: HeatMapValues) = {
    this.updateStatAccumulator(heatMapValues = value)
    value
  }
}
