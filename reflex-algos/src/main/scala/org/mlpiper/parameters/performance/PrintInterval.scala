package org.mlpiper.parameters.performance

import org.mlpiper.parameters.common.{PositiveLongParameter, WithArgumentParameters}

case object PrintInterval extends PositiveLongParameter {
  override val key = "printInterval"
  override val label: String = "Print Interval"
  override val defaultValue = Some(10000L)
  override val required = false
  override val description = "Number of samples to process per task slot before a metric/statistic is output from the pipeline. (Default: 10000)"
}

trait PrintInterval[Self] extends WithArgumentParameters {

  that: Self =>

  def setPrintInterval(interval: Long): Self = {
    this.parameters.add(PrintInterval, interval)
    that
  }
}
