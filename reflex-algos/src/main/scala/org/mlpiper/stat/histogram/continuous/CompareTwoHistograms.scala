package org.mlpiper.stat.histogram.continuous

import org.mlpiper.stat.histogram.{HistogramComparatorTypes, HistogramComparators}

import scala.collection.mutable

/**
  * Object [[CompareTwoHistograms]] is responsible for comparing two histograms.
  * Histograms will contain hist and binEdge details. Comparision will be done in following way
  *
  * <br> 1. Combine two bin edges using [[CombineBinEdges]].
  * <br> 2. Calculate new hist details for both histograms according to [[CombineBinEdges]].
  * <br> 3. Calculate score based on overlap method
  */
object CompareTwoHistograms {

  def compare(inferringHistogram: Histogram,
              contenderHistogram: Histogram,
              method: HistogramComparatorTypes.HistogramComparatorMethodType,
              addAdjustmentNormalizingEdge: Boolean)
  : Double = {
    var score: Double = 0.0
    // combining binEdge
    val combineBinEdge = CombineBinEdges.reduce(hist1 = contenderHistogram, hist2 = inferringHistogram)

    // calculating new hist detail based on combineBinEdge
    val newHistDetails = CombineHistDetails.getNewHistDetails(expectedBinEdge = combineBinEdge, hist1 = contenderHistogram, hist2 = inferringHistogram)

    val contenderHistDetails = newHistDetails.hist1Detail
    val inferringHistDetails = newHistDetails.hist2Detail

    val contenderHistCats: mutable.Map[String, Double] = mutable.Map[String, Double]().empty
    val inferringHistCats: mutable.Map[String, Double] = mutable.Map[String, Double]().empty

    // making map of string<->double to call comparators APIs
    combineBinEdge.toArray.zip(contenderHistDetails.toArray).foreach(tupleOfBinAndHistDetails => {
      contenderHistCats.put(tupleOfBinAndHistDetails._1.toString, tupleOfBinAndHistDetails._2)
    })

    combineBinEdge.toArray.zip(inferringHistDetails.toArray).foreach(tupleOfBinAndHistDetails => {
      inferringHistCats.put(tupleOfBinAndHistDetails._1.toString, tupleOfBinAndHistDetails._2)
    })

    score = HistogramComparators
      .compareHistograms(
        inferringHistCats = inferringHistCats.toMap,
        contenderHistCats = contenderHistCats.toMap,
        method = method,
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge
      )

    score
  }
}
