package org.mlpiper.parameters.common


trait PositiveIntParameter extends IntParameter {
  override val defaultValue: Option[Int] = None
  override lazy val errorMessage: String = key + " must be greater than zero"

  override def condition(x: Option[Int], parameters: ArgumentParameterChecker)
  : Boolean = {
    x.isDefined && x.get > 0
  }
}

trait PositiveLongParameter extends LongParameter {
  override val defaultValue: Option[Long] = None
  override lazy val errorMessage: String = key + " must be greater than zero"

  override def condition(x: Option[Long], parameters: ArgumentParameterChecker)
  : Boolean = {
    x.isDefined && x.get > 0L
  }
}

trait PositiveDoubleParameter extends DoubleParameter {
  override val defaultValue: Option[Double] = None
  override lazy val errorMessage: String = key + " must be greater than zero"

  override def condition(x: Option[Double], parameters: ArgumentParameterChecker)
  : Boolean = {
    x.isDefined && x.get > 0.0
  }
}
