package org.mlpiper.parameters.ml

import org.mlpiper.parameters.common.{PositiveIntParameter, WithArgumentParameters}

case object Attributes extends PositiveIntParameter {
  override val key = "attributes"
  override val label: String = "Attributes"
  override val required = true
  override val description = "Number of attributes in each sample"
}

trait Attributes[Self] extends WithArgumentParameters {

  that: Self =>

  def setAttributes(attributes: Int): Self = {
    this.parameters.add(Attributes, attributes)
    that
  }
}

