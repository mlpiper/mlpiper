package com.parallelmachines.reflex.test.common

import org.apache.flink.streaming.scala.examples.clustering.stat.{HistogramComparatorTypes, HistogramComparators}
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HistogramComparatorTest extends FlatSpec with Matchers {
  /**
    * Generate Correct Comparison Score For RMSE Overlap Without Adjustment
    */
  it should "Generate Correct Comparison Score For RMSE Overlap Without Adjustment" in {
    val contenderHist = Map[String, Double]("0.0" -> 0.4, "1.0" -> 0.2, "2.0" -> 0.3, "3.0" -> 0.1)

    val inferringHist = Map[String, Double]("0.0" -> 0.3, "1.0" -> 0.4, "2.0" -> 0.1, "3.0" -> 0.2)

    val score =
      HistogramComparators.compareHistograms(
        inferringHistCats = inferringHist,
        contenderHistCats = contenderHist,
        addAdjustmentNormalizingEdge = false,
        method = HistogramComparatorTypes.RMSE)

    // since, sum of contender = 1 and inferring is also 1.
    // rmse = sqrt((0.4 - 0.3)^2 + (0.2 - 0.4)^2 + (0.3 - 0.1)^2 + (0.1 - 0.2)^2)
    //      = 0.3163

    score should be(0.3163 +- 1e-2)
  }

  /**
    * Generate Correct Comparison Score For RMSE Overlap With Adjustment
    */
  it should "Generate Correct Comparison Score For RMSE Overlap With Adjustment" in {
    val contenderHist = Map[String, Double]("0.0" -> 0.4, "1.0" -> 0.2, "2.0" -> 0.3)

    val inferringHist = Map[String, Double]("0.0" -> 0.3, "1.0" -> 0.4, "2.0" -> 0.1)

    val score =
      HistogramComparators.compareHistograms(
        inferringHistCats = inferringHist,
        contenderHistCats = contenderHist,
        addAdjustmentNormalizingEdge = true,
        method = HistogramComparatorTypes.RMSE)

    // since, Adjustment is required, remaining in contender will be 0.1 and in inferring will be 0.2.
    // rmse = sqrt((0.4 - 0.3)^2 + (0.2 - 0.4)^2 + (0.3 - 0.1)^2 + (0.1 - 0.2)^2)
    //      = 0.3163

    score should be(0.3163 +- 1e-2)

    // even though, adjustment is true, it will not add new adjustment since sum is 1.0
    val contenderHistNonNorm = Map[String, Double]("0.0" -> 4.0, "1.0" -> 0.5, "2.0" -> 0.5)

    val inferringHistNonNorm = Map[String, Double]("0.0" -> 3.0, "1.0" -> 4.0, "2.0" -> 1.0)

    val scoreOfNonNorm =
      HistogramComparators.compareHistograms(
        inferringHistCats = inferringHistNonNorm,
        contenderHistCats = contenderHistNonNorm,
        addAdjustmentNormalizingEdge = true,
        method = HistogramComparatorTypes.RMSE)

    // since, Adjustment is required, remaining in contender will be 0.1 and in inferring will be 0.2.
    // rmse = sqrt((4/5 - 3/8)^2 + (0.5/5 - 4/8)^2 + (0.5/5 - 1/5)^2)
    //      = 0.5921

    scoreOfNonNorm should be(0.5921 +- 1e-2)
  }

  /**
    * Generate Correct Comparison Score For Probability Distribution Without Adjustment
    */
  it should "Generate Correct Comparison Score For Probability Distribution Method Without Adjustment" in {
    val contenderHist = Map[String, Double]("0.0" -> 0.4, "1.0" -> 0.2, "2.0" -> 0.3, "3.0" -> 0.1)

    val inferringHist = Map[String, Double]("0.0" -> 0.3, "1.0" -> 0.4, "2.0" -> 0.1, "3.0" -> 0.2)

    val score =
      HistogramComparators.compareHistograms(
        inferringHistCats = inferringHist,
        contenderHistCats = contenderHist,
        addAdjustmentNormalizingEdge = false,
        method = HistogramComparatorTypes.ProbabilityDistribution)

    // expectedScore = ((0.4 + 0.2 + 0.3 + 0.1) * (0.4 + 0.2 + 0.3 + 0.1) / (0.16 + 0.04 + 0.09 + 0.01)) * (((0.4 * 0.3) + (0.2 * 0.4) + (0.3 * 0.1) + (0.1 * 0.2) ) / ((0.4 + 0.2 + 0.3 + 0.1) * (0.3 + 0.4 + 0.1 + 0.2)))
    //               = 0.8333

    score should be(0.8333 +- 1e-2)
  }

  /**
    * Generate Correct Comparison Score For Probability Distribution With Adjustment
    */
  it should "Generate Correct Comparison Score For Probability Distribution Method With Adjustment" in {
    val contenderHist = Map[String, Double]("0.0" -> 0.4, "1.0" -> 0.2, "2.0" -> 0.3)

    val inferringHist = Map[String, Double]("0.0" -> 0.3, "1.0" -> 0.4, "2.0" -> 0.1)

    val score =
      HistogramComparators.compareHistograms(
        inferringHistCats = inferringHist,
        contenderHistCats = contenderHist,
        addAdjustmentNormalizingEdge = true,
        method = HistogramComparatorTypes.ProbabilityDistribution)

    // expectedScore = ((0.4 + 0.2 + 0.3 + 0.1) * (0.4 + 0.2 + 0.3 + 0.1) / (0.16 + 0.04 + 0.09 + 0.01)) * (((0.4 * 0.3) + (0.2 * 0.4) + (0.3 * 0.1) + (0.1 * 0.2) ) / ((0.4 + 0.2 + 0.3 + 0.1) * (0.3 + 0.4 + 0.1 + 0.2)))
    //               = 0.8333

    score should be(0.8333 +- 1e-2)

    // even though, adjustment is true, it will not add new adjustment since sum is 1.0
    val contenderHistNonNorm = Map[String, Double]("0.0" -> 4.0, "1.0" -> 0.5, "2.0" -> 0.5)

    val inferringHistNonNorm = Map[String, Double]("0.0" -> 3.0, "1.0" -> 4.0, "2.0" -> 1.0)

    val scoreOfNonNorm =
      HistogramComparators.compareHistograms(
        inferringHistCats = inferringHistNonNorm,
        contenderHistCats = contenderHistNonNorm,
        addAdjustmentNormalizingEdge = true,
        method = HistogramComparatorTypes.ProbabilityDistribution)

    // expectedScore = ((4.0 + 0.5 + 0.5) * (4.0 + 0.5 + 0.5) / (16.0 + 0.25 + 0.25)) * (((4.0 * 3.0) + (0.5 * 4.0) + (0.5 * 1.0)) / ((3.0 + 4.0 + 1.0) * (4.0 + 0.5 + 0.5)))
    //               = 0.5493

    scoreOfNonNorm should be(0.5493 +- 1e-2)
  }
}
