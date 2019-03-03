package org.mlpiper.stat.histogram.categorical

import org.mlpiper.stats.GraphFormat

/**
  * class represents Histogram of Doubles for categorical values
  * It Holds the map of categorical values and count of each category
  */
class Histogram(categoricalCounts: Map[String, Double]) extends Serializable with GraphFormat {

  def getCategoricalCount: Map[String, Double] = categoricalCounts

  def getDataMap: Map[String, Double] = categoricalCounts

  /**
    * method will convert histogram to string representation
    */
  override def toString: String = {
    toGraphString()
  }
}
