package org.mlpiper.stat.histogram.categorical

import org.mlpiper.stat.healthlib.{HealthConfEnum, HealthConfigurations}

import scala.collection.mutable

/** TODO: https://parallelmachines.atlassian.net/browse/REF-1889
  * 1. Normalization and selection of limited features should be part of associated histogram classes
  * 2. Formatting like precision or any other things should be part of formatting class only
  */
object HistogramFormatting extends Serializable {
  private val numberPrecision: Int = HealthConfigurations.getConf(HealthConfEnum.NumberPrecision).asInstanceOf[Int]
  private val enableNumberPrecision: Boolean = HealthConfigurations.getConf(HealthConfEnum.EnableNumberPrecision).asInstanceOf[Boolean]

  def formatHistogram(histogram: Histogram,
                      enableNormalization: Boolean,
                      setOfPredefinedCategories: Option[Set[String]]): Histogram = {

    val maxCategoriesToConsider: Int = HealthConfigurations.getConf(HealthConfEnum.MaxCategoryInHistogram).asInstanceOf[Int]

    val categoricalHistogram: mutable.Map[String, Double] = mutable.Map[String, Double]() ++ histogram.getCategoricalCount

    val allCategories = categoricalHistogram.keys

    if (enableNormalization) {
      val sumOfAllCategories = categoricalHistogram.values.sum

      allCategories.foreach(eachCat => {
        categoricalHistogram(eachCat) = categoricalHistogram(eachCat) / sumOfAllCategories
      })
    }

    if (enableNumberPrecision) {
      allCategories.foreach(eachCat => {
        categoricalHistogram(eachCat) = BigDecimal.decimal(categoricalHistogram(eachCat)).setScale(numberPrecision, BigDecimal.RoundingMode.HALF_UP).toDouble
      })
    }

    // filtering categories based on provided categories
    val filteredMapOfBinAndEdgeRep =
      if (setOfPredefinedCategories.isDefined) {
        val predefinedCategories = setOfPredefinedCategories.get
        categoricalHistogram.filterKeys(keys => predefinedCategories.contains(keys))
      } else {
        categoricalHistogram
      }

    val sizeOfCategories = filteredMapOfBinAndEdgeRep.size

    // selecting top categories only if size exceeds provided maximum count
    val sortedMapOfBinAndEdgeRep =
      if (sizeOfCategories > maxCategoriesToConsider) {
        filteredMapOfBinAndEdgeRep.toList.sortWith((x, y) => x._2 > y._2).slice(0, maxCategoriesToConsider)
      } else {
        filteredMapOfBinAndEdgeRep.toList
      }

    new Histogram(categoricalCounts = sortedMapOfBinAndEdgeRep.toMap)
  }

  def formatFeaturedHistogram(featuredHistogram: mutable.Map[String, Histogram],
                              enableNormalization: Boolean,
                              setOfPredefinedCategoriesForFeatures: Option[Map[String, Set[String]]]): mutable.Map[String, Histogram] = {
    val featuredHistogramMap: mutable.Map[String, Histogram] = mutable.Map[String, Histogram]() ++ featuredHistogram

    val features = featuredHistogramMap.keys

    features.foreach(eachFeature => {
      featuredHistogramMap(eachFeature) =
        formatHistogram(
          featuredHistogramMap(eachFeature),
          enableNormalization = enableNormalization,
          setOfPredefinedCategories =
            if (setOfPredefinedCategoriesForFeatures.isDefined &&
              setOfPredefinedCategoriesForFeatures.get.contains(eachFeature))
              Some(setOfPredefinedCategoriesForFeatures.get(eachFeature))
            else None
        )
    })

    featuredHistogramMap
  }

  def getFeaturedCategoryMap(featuredHistogram: mutable.Map[String, Histogram]): Map[String, Set[String]] = {
    featuredHistogram.map(x =>
      (x._1,
        // creating set of categories from contender
        x._2.getCategoricalCount.keys.toSet))
      .toMap
  }
}
