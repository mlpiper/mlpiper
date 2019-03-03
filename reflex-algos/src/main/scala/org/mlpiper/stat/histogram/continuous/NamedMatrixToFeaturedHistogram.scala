package org.mlpiper.stat.histogram.continuous

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.common.InfoType.InfoType
import org.mlpiper.stats._
import org.mlpiper.datastructures.NamedMatrix
import org.slf4j.LoggerFactory

import scala.collection.mutable

object NamedMatrixToFeaturedHistogram {

  private val logger = LoggerFactory.getLogger(getClass)

  val DefaultBinSize: Double = 10.0

  def getAccumulator(accumName: String, infoType: InfoType, modelId: String, startingHist: HistogramWrapper)
  : GlobalAccumulator[HistogramWrapper] = {
    new GlobalAccumulator[HistogramWrapper](
      name = accumName,
      localMerge = (_: AccumulatorInfo[HistogramWrapper],
                    newHist: AccumulatorInfo[HistogramWrapper]) => {
        newHist
      },
      globalMerge = (x: AccumulatorInfo[HistogramWrapper],
                     y: AccumulatorInfo[HistogramWrapper]) => {
        AccumulatorInfo(
          value = HistogramWrapper(
            CombineFeaturedHistograms.combineTwoFeaturedHistograms(x.value.histogram, y.value.histogram)),
          count = x.count + y.count,
          accumModeType = x.accumModeType,
          accumGraphType = x.accumGraphType,
          name = x.name,
          infoType = x.infoType,
          modelId = x.modelId)
      },
      startingValue = startingHist,
      accumDataType = AccumData.BarGraph,
      accumModeType = AccumMode.Instant,
      infoType = infoType,
      modelId = modelId
    )
  }

  /**
    * Method is responsible for generating Min and Max bin range from given featuredHistogram.
    * Method will also create bin size requirement for each features.
    * Map representation has string key which will represent feature ID.
    */
  def generateMinMaxRangeAndBinSizeFromFeaturedHist(featuredHistogram: mutable.Map[String, Histogram],
                                                    excludeGaurdedBin: Boolean = true)
  : (mutable.Map[String, Double], mutable.Map[String, Double], mutable.Map[String, Double]) = {
    val guardedBins: Int = if (excludeGaurdedBin) 1 else 0

    // min and max map will be for each feature. So creating list of size of arrayOfBinEdgesOfEachFeature
    val minBinListForEachFeature: mutable.Map[String, Double] = mutable.Map[String, Double]()
    val maxBinListForEachFeature: mutable.Map[String, Double] = mutable.Map[String, Double]()
    val binSizeForEachFeature: mutable.Map[String, Double] = mutable.Map[String, Double]()

    val mapOfFeatureAndBin: mutable.Map[String, DenseVector[Double]] = featuredHistogram.map(x => (x._1, x._2.binEdges))

    //  filling up min and max bin map
    mapOfFeatureAndBin.foreach(x => {
      minBinListForEachFeature(x._1) = x._2(0 + guardedBins)
      maxBinListForEachFeature(x._1) = x._2(x._2.length - 1 - guardedBins)
      binSizeForEachFeature(x._1) = x._2.length - 1 - (2 * guardedBins)
    })

    (minBinListForEachFeature, maxBinListForEachFeature, binSizeForEachFeature)
  }

  /**
    * Method/API is responsible for creating Featured Histogram from NamedMatrix for given bin ranges of each attributes.
    */
  def apply(namedMatrixRep: NamedMatrix,
            binSizeForEachFeatureForRef: Option[Map[String, Double]] = None,
            minBinValueForEachFeatureForRef: Option[Map[String, Double]],
            maxBinValueForEachFeatureForRef: Option[Map[String, Double]]): mutable.Map[String, Histogram] = {

    val vectorEntries = namedMatrixRep.arrayOfVector

    // map of featureID and histogram
    val mapOfFeatureIDAndHist = mutable.Map[String, Histogram]()

    for (eachFeaturesVector <- vectorEntries) {
      try {
        eachFeaturesVector.columnValue match {

          case innerVector: DenseVector[_] =>
            val nonNullInnerVector = DenseVector(innerVector.toArray.filter(x => x != null && x != None))

            val featureName = eachFeaturesVector.columnName

            // selecting bin size if provided
            val binSize: Option[Double] = if (binSizeForEachFeatureForRef.isDefined) {
              binSizeForEachFeatureForRef.get.get(featureName)
            } else {
              None
            }

            // selecting min range if provided
            val lowerBound: Option[Double] = if (minBinValueForEachFeatureForRef.isDefined) {
              minBinValueForEachFeatureForRef.get.get(featureName)
            } else {
              None
            }

            // selecting max range if provided
            val upperBound: Option[Double] = if (maxBinValueForEachFeatureForRef.isDefined) {
              maxBinValueForEachFeatureForRef.get.get(featureName)
            } else {
              None
            }

            // calculate histogram of given DenseVector
            val vectorHistogram: Option[Histogram] =
              FeaturedHistogramFromDenseVector.getHistogram(vector = nonNullInnerVector,
                binSize = binSize,
                lowerBound = lowerBound,
                upperBound = upperBound)

            if (vectorHistogram.isDefined) {
              mapOfFeatureIDAndHist += (featureName -> vectorHistogram.get)
            }

          case _ =>
            logger.info(s"In today's world, We are sorry.. but we cannot create histogram for feature ${eachFeaturesVector.columnName}")
        }
      }
      catch {
        case exception: Exception =>
          exception.printStackTrace()

        case throwable: Throwable =>
          throwable.printStackTrace()
      }
    }

    mapOfFeatureIDAndHist
  }
}
