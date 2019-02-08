package org.mlpiper.stat.histogram.categorical

import scala.collection.mutable

/**
  * Object CombineHistograms is responsible for combining two histograms.
  * Histograms will contain hist and binEdge details. Each bin represent the data of categorical set.
  *
  */
object CombineHistograms {
  def reduce(hist1: Histogram, hist2: Histogram)
  : Histogram = {

    val combinedMapOfCategoryAndCount: mutable.Map[String, Double] = mutable.Map[String, Double]()

    val totalUniqueCategories = hist1.getCategoricalCount.keys.toSet.union(hist2.getCategoricalCount.keys.toSet)

    totalUniqueCategories.foreach(eachCategory => {
      val combinedCountOfCategory: Double = hist1.getCategoricalCount.getOrElse(eachCategory, 0.0) + hist2.getCategoricalCount.getOrElse(eachCategory, 0.0)

      combinedMapOfCategoryAndCount(eachCategory) = combinedCountOfCategory
    })

    new Histogram(categoricalCounts = combinedMapOfCategoryAndCount.toMap)
  }
}
