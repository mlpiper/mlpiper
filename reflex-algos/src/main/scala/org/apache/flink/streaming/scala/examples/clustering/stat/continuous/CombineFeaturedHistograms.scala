package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import org.apache.flink.api.common.functions.ReduceFunction

import scala.collection.mutable

/**
  * class is responsible for providing reduce functionality to combine featured histograms
  */
class CombineFeaturedHistograms
  extends ReduceFunction[mutable.Map[String, Histogram]] {

  override def reduce(map1OfFeatureHist: mutable.Map[String, Histogram],
                      map2OfFeatureHist: mutable.Map[String, Histogram])
  : mutable.Map[String, Histogram] = {
    val combinedFeatureIDAndHist = CombineFeaturedHistograms.combineTwoFeaturedHistograms(map1OfFeatureHist = map1OfFeatureHist, map2OfFeatureHist = map2OfFeatureHist)

    combinedFeatureIDAndHist
  }
}

object CombineFeaturedHistograms {
  def combineTwoFeaturedHistograms(map1OfFeatureHist: mutable.Map[String, Histogram],
                                   map2OfFeatureHist: mutable.Map[String, Histogram])
  : mutable.Map[String, Histogram] = {
    // get set of featureIDs - both maps will have same keys, so choosing either's keys
    val setOfFeatureIDs = map1OfFeatureHist.keys.toSet.union(map2OfFeatureHist.keys.toSet)

    val combinedFeatureIDAndHist = mutable.Map[String, Histogram]()

    for (eachFeatureID <- setOfFeatureIDs) {
      // calculating combined histogram for each feature
      val combinedHist = {
        if (map1OfFeatureHist.get(eachFeatureID).isEmpty) {
          map2OfFeatureHist(eachFeatureID)
        } else if (map2OfFeatureHist.get(eachFeatureID).isEmpty) {
          map1OfFeatureHist(eachFeatureID)
        } else {
          CombineHistograms.reduce(map1OfFeatureHist(eachFeatureID),
            map2OfFeatureHist(eachFeatureID))
        }
      }

      // populate map of combined map of featured histograms
      combinedFeatureIDAndHist += (eachFeatureID -> combinedHist)
    }
    combinedFeatureIDAndHist
  }
}
