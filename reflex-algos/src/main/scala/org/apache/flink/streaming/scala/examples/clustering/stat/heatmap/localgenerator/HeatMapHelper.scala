package org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.localgenerator

import org.apache.flink.streaming.scala.examples.clustering.math.ReflexNamedVector
import org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.HeatMapValues

abstract class HeatMapHelper extends Serializable {
  def generateHeatMap(value: Iterable[ReflexNamedVector]): HeatMapValues
}
