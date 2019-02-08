package org.mlpiper.parameters.pipeline

import org.mlpiper.parameters.common.{ArgumentParameterChecker, BooleanParameter, PositiveLongParameter, WithArgumentParameters}

case object Checkpointing extends BooleanParameter {
  override val key = "enableCheckpointing"
  override val label: String = "Enable Checkpointing"
  override val required = false
  override val description: String = validBooleanParameters + "Toggles checkpointing"

  override def condition(enableCheckpointing: Option[Boolean],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    super.condition(enableCheckpointing, parameters) && parameters.contains(CheckpointingInterval)
  }
}

case object CheckpointingInterval extends PositiveLongParameter {
  override val key = "checkpointInterval"
  override val label = "Checkpoint Interval"
  override val required = false
  override val description = "Interval in seconds (or milliseconds) before a checkpoint is taken"

  override def condition(interval: Option[Long],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    super.condition(interval, parameters) && parameters.contains(Checkpointing)
  }
}

trait Checkpointing[Self] extends WithArgumentParameters {

  that: Self =>

  def enableCheckpointing(enableCheckpointing: Boolean): Self = {
    this.parameters.add(Checkpointing, enableCheckpointing)
    that
  }

  def enableCheckpointing(checkpointInterval: Long): Self = {
    this.parameters.add(Checkpointing, true)
    this.parameters.add(CheckpointingInterval, checkpointInterval)
    that
  }
}
