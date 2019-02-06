package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

/**
  * Object CombineHistograms is responsible for combining two histograms.
  * Histograms will contain hist and binEdge details. Merging will be done in following way
  *
  * <br> 1. Combine two bin edges using [[CombineBinEdges]].
  * <br> 2. Populate hist details from both histogram according to [[CombineBinEdges]].
  * <br> 3. Shrink down histogram based on origianal size.
  */
object CombineHistograms {
  def reduce(hist1: Histogram, hist2: Histogram)
  : Histogram = {
    // both hist's bin edges length will be usually same - but averaging the length
    val originalHistLength = (hist1.binEdges.length + hist2.binEdges.length) / 2

    // combining binEdge
    val combineBinEdge = CombineBinEdges.reduce(hist1 = hist1, hist2 = hist2)

    // calculate hist details from both hists
    val combineHistDetail = CombineHistDetails.reduce(combineBinEdge = combineBinEdge, hist1 = hist1, hist2 = hist2)

    val combinedHist = new Histogram(histVector = combineHistDetail, binEdgesVector = combineBinEdge)

    // shrink combined hist to original size
    val shrinkHist = ShrinkHistogram.reduce(hist = combinedHist, shrinkSize = originalHistLength)
    shrinkHist
  }
}
