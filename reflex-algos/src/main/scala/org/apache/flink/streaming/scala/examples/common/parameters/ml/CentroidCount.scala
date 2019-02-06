package org.apache.flink.streaming.scala.examples.common.parameters.ml

import org.apache.flink.streaming.scala.examples.common.parameters.common.PositiveIntParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.WithArgumentParameters

case object CentroidCount extends PositiveIntParameter {
  override val key = "centroidCount"
  override val label: String = "Centroids"
  override val required = true
  override val description = "Number of centroids"
}

trait CentroidCount[Self] extends WithArgumentParameters {

  that: Self =>

  def setCentroidCount(centroidCount: Int): Self = {
    this.parameters.add(CentroidCount, centroidCount)
    that
  }
}