package org.mlpiper.stat.histogram

import org.slf4j.LoggerFactory

object HistogramComparators {

  // Comparision formula is as following.
  //         square of(sum of contender histogram counts)      sum(contender histogram count * inference histogram count)
  // score = -------------------------------------------- * -----------------------------------------------------------------
  //          sum(square of contender histogram counts)     sum(contender histogram counts) * sum(inference histogram counts)
  private def compareHistUsingProbabilityDistribution(inferringHistCats: Map[String, Double],
                                                      contenderHistCats: Map[String, Double],
                                                      addAdjustmentNormalizingEdge: Boolean)
  : Double = {
    var score: Double = 0.0
    // variable will hold sum of multiplication of both histogram counts
    var sumOfMultOfBothHistCount: Double = 0.0
    // variable will hold sum of square of referenced histogram counts
    var sumOfSquareOfContenderHistCount: Double = 0.0
    // variable will hold sum of referenced histogram counts
    var sumOfContenderHistCount: Double = 0.0
    // variable will hold sum of contender histogram counts
    var sumOfInferencedHistCount: Double = 0.0

    val totalCategories = contenderHistCats.keys.toSet.union(inferringHistCats.keys.toSet)

    totalCategories.foreach(eachCat => {
      val contenderHistCount = contenderHistCats.getOrElse(eachCat, 0.0)
      val infHistCount = inferringHistCats.getOrElse(eachCat, 0.0)

      sumOfContenderHistCount += contenderHistCount
      sumOfInferencedHistCount += infHistCount

      sumOfMultOfBothHistCount += contenderHistCount * infHistCount
      sumOfSquareOfContenderHistCount += contenderHistCount * contenderHistCount
    })

    /**
      * This block will be responsible for calculation the score for remaining categories which could not included in histograms
      * Maximum sum of count for all normalized category count is 1.
      */
    if (addAdjustmentNormalizingEdge && sumOfContenderHistCount <= 1.0 && sumOfInferencedHistCount <= 1.0) {
      val maxNormalizedCategoriesCount: Double = 1.0
      val remainingContenderHistCount = maxNormalizedCategoriesCount - sumOfContenderHistCount
      val remainingInferencedHistCount = maxNormalizedCategoriesCount - sumOfInferencedHistCount

      sumOfContenderHistCount = maxNormalizedCategoriesCount
      sumOfInferencedHistCount = maxNormalizedCategoriesCount

      sumOfMultOfBothHistCount += remainingContenderHistCount * remainingInferencedHistCount
      sumOfSquareOfContenderHistCount += remainingContenderHistCount * remainingContenderHistCount
    }

    if (sumOfSquareOfContenderHistCount != 0 && sumOfContenderHistCount != 0 && sumOfInferencedHistCount != 0) {
      val contenderHistReferenceValue = (sumOfContenderHistCount * sumOfContenderHistCount) / sumOfSquareOfContenderHistCount
      val inferenceHistReferenceValue = sumOfMultOfBothHistCount / (sumOfContenderHistCount * sumOfInferencedHistCount)
      score = contenderHistReferenceValue * inferenceHistReferenceValue
    }

    // clipping score if it goes beyond 1.0
    if (score > 1.0) {
      score = 1.0
    }

    score
  }


  /**
    * Method will calculate rmse between two hist details
    * rmse can be calculated by following equation
    *
    * rmse = sqrt((x1-x2)^^2/n)
    */
  private def compareHistUsingRMSEScore(inferringHistCats: Map[String, Double],
                                        contenderHistCats: Map[String, Double],
                                        addAdjustmentNormalizingEdge: Boolean): Double = {
    var score: Double = 0.0

    var sumOfContenderHistCount = contenderHistCats.values.sum
    var sumOfInferencedHistCount = inferringHistCats.values.sum

    // setting totalSums to 1s if totalSums generated is zero. It will happen only if hist detail of histogram is zeros. (i.e. no graph at all)
    if (sumOfContenderHistCount == 0) {
      sumOfContenderHistCount = 1.0
    }
    if (sumOfInferencedHistCount == 0) {
      sumOfInferencedHistCount = 1.0
    }

    val rmseBase: Int = 2

    /**
      * This block will be responsible for calculation the score for remaining categories which could not included in histograms
      * Maximum sum of count for all normalized category count is 1.
      */
    if (addAdjustmentNormalizingEdge && sumOfContenderHistCount <= 1.0 && sumOfInferencedHistCount <= 1.0) {
      val maxNormalizedCategoriesCount: Double = 1.0
      val remainingContenderHistCount = maxNormalizedCategoriesCount - sumOfContenderHistCount
      val remainingInferencedHistCount = maxNormalizedCategoriesCount - sumOfInferencedHistCount

      sumOfContenderHistCount = maxNormalizedCategoriesCount
      sumOfInferencedHistCount = maxNormalizedCategoriesCount

      val normedContenderHistCount: Double = remainingContenderHistCount / sumOfContenderHistCount
      val normedInferringHistCount: Double = remainingInferencedHistCount / sumOfInferencedHistCount

      score += math.pow(normedContenderHistCount - normedInferringHistCount, rmseBase)

    }
    val totalCategories = contenderHistCats.keys.toSet.union(inferringHistCats.keys.toSet)

    totalCategories.foreach(eachCat => {
      // calculating score only for bins which contains some hist details on either of histograms
      if (!(contenderHistCats(eachCat) == 0 && inferringHistCats(eachCat) == 0)) {
        val normedContenderHistCount: Double = contenderHistCats(eachCat) / sumOfContenderHistCount
        val normedInferringHistCount: Double = inferringHistCats(eachCat) / sumOfInferencedHistCount

        score += math.pow(normedContenderHistCount - normedInferringHistCount, rmseBase)
      }
    })

    math.sqrt(score)
  }

  def compareHistograms(inferringHistCats: Map[String, Double],
                        contenderHistCats: Map[String, Double],
                        addAdjustmentNormalizingEdge: Boolean,
                        method: HistogramComparatorTypes.HistogramComparatorMethodType): Double = {
    val score: Double = method match {
      case HistogramComparatorTypes.RMSE =>
        this.compareHistUsingRMSEScore(
          inferringHistCats = inferringHistCats,
          contenderHistCats = contenderHistCats,
          addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

      case HistogramComparatorTypes.ProbabilityDistribution =>
        this.compareHistUsingProbabilityDistribution(
          inferringHistCats = inferringHistCats,
          contenderHistCats = contenderHistCats,
          addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

      case _ =>
        LoggerFactory.getLogger(getClass).error(s"$method provided is invalid")
        0.0
    }

    score
  }
}
