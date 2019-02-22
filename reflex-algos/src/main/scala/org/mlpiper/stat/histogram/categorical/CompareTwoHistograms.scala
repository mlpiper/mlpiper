package org.mlpiper.stat.histogram.categorical

import org.mlpiper.stat.histogram.{HistogramComparatorTypes, HistogramComparators}

/**
  * Object [[CompareTwoHistograms]] is responsible for comparing two histograms.
  * Histograms will contain Categorical representation.
  */
object CompareTwoHistograms {

  def compare(contenderHistogram: Histogram,
              inferringHistogram: Histogram,
              method: HistogramComparatorTypes.HistogramComparatorMethodType,
              addAdjustmentNormalizingEdge: Boolean)
  : Double = {
    val contenderHistCats: Map[String, Double] = contenderHistogram.getCategoricalCount
    val inferringHistCats: Map[String, Double] = inferringHistogram.getCategoricalCount

    var score: Double = 0.0

    score = HistogramComparators
      .compareHistograms(
        inferringHistCats = inferringHistCats,
        contenderHistCats = contenderHistCats,
        method = method,
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge
      )

    score
  }
}
