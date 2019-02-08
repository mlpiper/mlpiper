package org.mlpiper.parameters.ml

import org.mlpiper.parameters.common.{BooleanParameter, WithArgumentParameters}

case object Validation extends BooleanParameter {
  override val key = "enableValidation"
  override val label: String = "Enable Validation"
  override val required = false
  override val description = "Toggles validation mode. (Default: false)"
}

trait Validation[Self] extends WithArgumentParameters {

  that: Self =>

  def enableValidation(enableValidation: Boolean): Self = {
    this.parameters.add(Validation, enableValidation)
    that
  }
}

case object ValidationAccumulators extends BooleanParameter {
  override val key = "enableAccumulators"
  override val label: String = "Enable Accumulators for validation"
  override val required = false
  override val description = "Toggles use of accumulators for validation. (Default: false)"
}

trait ValidationAccumulators[Self] extends WithArgumentParameters {

  that: Self =>

  def enableAccumulators(enableAccumulators: Boolean): Self = {
    this.parameters.add(ValidationAccumulators, enableAccumulators)
    that
  }
}
