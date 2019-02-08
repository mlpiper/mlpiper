package org.mlpiper.stat.heatmap.continuous.localgenerator

import org.mlpiper.datastructures.NamedVector
import org.mlpiper.stat.heatmap.continuous.HeatMapValues

abstract class HeatMapHelper extends Serializable {
  def generateHeatMap(value: Iterable[NamedVector]): HeatMapValues
}
