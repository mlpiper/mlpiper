package org.mlpiper.utils

object Constants {
  /** New line for any environment. */
  val NewLine: String = System.getProperty("line.separator")
}

object GenericConstants {
  var DataFrameColumnarSliceSize: Int = 100
  var MaximumCategoricalUniqueValue: Int = 25
}
