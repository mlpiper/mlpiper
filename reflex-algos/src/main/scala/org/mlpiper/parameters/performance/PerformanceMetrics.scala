package org.mlpiper.parameters.performance

import org.mlpiper.parameters.common.{ArgumentParameterChecker, BooleanParameter, WithArgumentParameters}

case object PerformanceMetrics extends BooleanParameter {
  override val key = "enablePerformance"
  override val label: String = "Enable Perf. Measurments"
  override val required = false
  override val description: String = "Switch to enable collection of performance metrics. (Default: false)"
  override lazy val errorMessage: String = key + " must be true or false. " + "" +
    "If " + key + " is true, " + PrintInterval.key + " must be defined."

  override def condition(performance: Option[Boolean],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    if (performance.isDefined) {
      if (performance.get) {
        parameters.contains(PrintInterval)
      } else {
        true
      }
    } else {
      false
    }
  }
}

trait PerformanceMetrics[Self] extends WithArgumentParameters {

  that: Self =>

  def enablePerformanceMetrics(enablePerformanceMode: Boolean): Self = {
    this.parameters.add(PerformanceMetrics, enablePerformanceMode)
    that
  }
}
