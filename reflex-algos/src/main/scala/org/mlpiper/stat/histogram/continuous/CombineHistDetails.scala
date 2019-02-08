package org.mlpiper.stat.histogram.continuous

import breeze.linalg.DenseVector

/**
  * Object CombineHistDetails is responsible for creating new Hist details based on combined bin
  * It will populate hist details from both histogram according to CombinedBinEdge
  */
object CombineHistDetails {

  /** method will calculate portion of edge to use based on expected range provided */
  def calculateHistDetailToPopulate(histToUse: Histogram,
                                    expectedRange: Double,
                                    indexOfBinEdge: Int): Double = {
    val histBinEdge = histToUse.binEdges
    val histHistDetail = histToUse.hist

    val rangeOfHistBinEdge = histBinEdge(indexOfBinEdge + 1) - histBinEdge(indexOfBinEdge)

    // hist will contribute portion of detail to combined hist detail according to range it falls into
    val histDetailToGo = (expectedRange * histHistDetail(indexOfBinEdge)) / rangeOfHistBinEdge

    histDetailToGo
  }

  /** method will create new hist details for two histograms based on expected bin edge provided */
  def getNewHistDetails(expectedBinEdge: DenseVector[Double],
                        hist1: Histogram,
                        hist2: Histogram): NewHistDetails = {
    var indexCombineBinEdge = 0
    val lengthCombineBinEdge = expectedBinEdge.length

    val hist1BinEdge = hist1.binEdges
    val lengthHist1BinEdge = hist1BinEdge.length
    var indexHist1BinEdge = 0

    val newHist1Detail = new Array[Double](expectedBinEdge.length - 1)

    val hist2BinEdge = hist2.binEdges
    val lengthHist2BinEdge = hist2BinEdge.length
    var indexHist2BinEdge = 0

    val newHist2Detail = new Array[Double](expectedBinEdge.length - 1)

    // looping through combinedBinEdge till the last element
    while (indexCombineBinEdge < lengthCombineBinEdge - 1) {
      val nextIndexOfCombineBinEdge = indexCombineBinEdge + 1
      val rangeOfCombineBinEdge = expectedBinEdge(nextIndexOfCombineBinEdge) - expectedBinEdge(indexCombineBinEdge)

      // populate hist detail for combine histogram from histogram 1 only if histogram 1's detail falls into combined bin bucket
      if (indexHist1BinEdge < lengthHist1BinEdge - 1
        && hist1BinEdge(indexHist1BinEdge) <= expectedBinEdge(indexCombineBinEdge)) {

        // populate combined hist details with hist1's portion
        newHist1Detail(indexCombineBinEdge) += calculateHistDetailToPopulate(histToUse = hist1, expectedRange = rangeOfCombineBinEdge, indexOfBinEdge = indexHist1BinEdge)

        // updating bin to look for for hist1 if hist1 is fully done satisfying combine hist's current bin
        if (hist1BinEdge(indexHist1BinEdge + 1) <= expectedBinEdge(nextIndexOfCombineBinEdge)) {
          indexHist1BinEdge += 1
        }
      }

      // populate hist detail for combine histogram from histogram 2 only if histogram 2's detail falls into combined bin bucket
      if (indexHist2BinEdge < lengthHist2BinEdge - 1
        && hist2BinEdge(indexHist2BinEdge) <= expectedBinEdge(indexCombineBinEdge)) {

        // populate combined hist details with hist2's portion
        newHist2Detail(indexCombineBinEdge) += calculateHistDetailToPopulate(histToUse = hist2, expectedRange = rangeOfCombineBinEdge, indexOfBinEdge = indexHist2BinEdge)

        // updating bin to look for for hist1 if hist2 is fully done satisfying combine hist's current bin
        if (hist2BinEdge(indexHist2BinEdge + 1) <= expectedBinEdge(nextIndexOfCombineBinEdge)) {
          indexHist2BinEdge += 1
        }
      }

      indexCombineBinEdge += 1
    }

    NewHistDetails(hist1Detail = new DenseVector[Double](newHist1Detail), hist2Detail = new DenseVector[Double](newHist2Detail))
  }

  /** case will hold new hist details for both histograms */
  case class NewHistDetails(hist1Detail: DenseVector[Double], hist2Detail: DenseVector[Double])

  def reduce(combineBinEdge: DenseVector[Double], hist1: Histogram, hist2: Histogram)
  : DenseVector[Double] = {
    val newHistDetails = getNewHistDetails(expectedBinEdge = combineBinEdge, hist1 = hist1, hist2 = hist2)

    val hist1HistDetails = newHistDetails.hist1Detail
    val hist2HistDetails = newHistDetails.hist2Detail

    val combineHistDetail = hist1HistDetails :+ hist2HistDetails
    combineHistDetail
  }
}
