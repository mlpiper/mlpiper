package org.mlpiper.stat.histogram.continuous

import org.mlpiper.stat.histogram.HistogramComparatorTypes
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * object is responsible for providing compare functionality to compare two featured histograms
  * each score will be calculated individually for each feature and then returning Map containing scores associated with each feature ID
  */
object CompareTwoFeaturedHistograms {
  private val logger = LoggerFactory.getLogger(getClass)

  def compare(inferencefeaturedHistogram: mutable.Map[String, Histogram],
              contenderfeaturedHistogram: mutable.Map[String, Histogram],
              method: HistogramComparatorTypes.HistogramComparatorMethodType,
              addAdjustmentNormalizingEdge: Boolean)
  : Map[String, Double] = {
    // get set of featureIDs - both maps can have different keys. So choosing intersaction of both
    val featureIDs = contenderfeaturedHistogram.keys.toSet.intersect(inferencefeaturedHistogram.keys.toSet)

    // stat to hold score for each features
    val overlapScoreOfFeatures = mutable.Map[String, Double]()

    if (featureIDs.isEmpty) {
      logger.error(s"Features list cannot be zero for comparision.\n" +
        s"First histogram's provided feature IDs are ${contenderfeaturedHistogram.keys.mkString(", ")}.\n" +
        s"Second histogram's provided feature IDs are ${inferencefeaturedHistogram.keys.mkString(", ")}.")
    }

    for (eachFeatureID <- featureIDs) {
      // calculating score for each feature's histograms using [[CompareTwoHistograms]] object
      val eachFeatureScore: Double =
        CompareTwoHistograms.compare(inferringHistogram = inferencefeaturedHistogram(eachFeatureID),
          contenderHistogram = contenderfeaturedHistogram(eachFeatureID),
          method = method,
          addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

      overlapScoreOfFeatures(eachFeatureID) = eachFeatureScore
    }

    // always return immutable map so that, it cannot be changed once scores are pinned to each feature for given set of histograms
    overlapScoreOfFeatures.toMap
  }
}
