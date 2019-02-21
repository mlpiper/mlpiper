package org.apache.flink.streaming.scala.examples.clustering.stat

/** Object represents type of HistogramComparatorTypes functions available for comparing two hist */
object HistogramComparatorTypes extends Enumeration {
  type HistogramComparatorMethodType = Value
  val RMSE: Value = Value("rmse")
  val ProbabilityDistribution: Value = Value("probability distribution")

  def contains(overlapMethod: String): Boolean = values.exists(_.toString == overlapMethod)

  override def toString: String = s"Supported Overlap Methods: ${values.mkString(", ")}"
}
