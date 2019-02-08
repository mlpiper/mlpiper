package org.mlpiper.stat.histogram.categorical

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.common.InfoType.InfoType
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.scala.examples.common.stats._
import org.mlpiper.datastructures.NamedMatrix
import org.slf4j.LoggerFactory

import scala.collection.mutable

object NamedMatrixToFeaturedHistogram {

  private val logger = LoggerFactory.getLogger(getClass)

  def getAccumulator(accumName: String, infoType: InfoType, modelId: String, startingHist: HistogramWrapper)
  : GlobalAccumulator[HistogramWrapper] = {
    new GlobalAccumulator[HistogramWrapper](
      // right now outputing to categorical until we include dataType in accums
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
          modelId = x.modelId,
          infoType = x.infoType)
      },
      startingValue = startingHist,
      accumDataType = AccumData.BarGraph,
      accumModeType = AccumMode.Instant,
      infoType = infoType,
      modelId = modelId
    )
  }

  /**
    * Method/API is responsible for creating Featured Histogram from NamedMatrix.
    */
  def apply(namedMatrixRep: NamedMatrix): mutable.Map[String, Histogram] = {

    val vectorEntries = namedMatrixRep.arrayOfVector

    // map of featureID and histogram
    val mapOfFeatureIDAndHist = mutable.Map[String, Histogram]()

    for (eachFeaturesVector <- vectorEntries) {
      try {
        eachFeaturesVector.columnValue match {

          case innerVector: DenseVector[_] =>
            val nonNullInnerVector = DenseVector(innerVector.toArray.filter(x => x != null && x != None))

            val featureName = eachFeaturesVector.columnName

            // calculate histogram of given DenseVector
            val vectorHistogram: Option[Histogram] =
              FeaturedHistogramFromDenseVector.getHistogram(vector = nonNullInnerVector)

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

/**
  * Compute histogram on DataStream of NamedVector which contains Matrix Representation.
  * Functionality is responsible for creating histogram for categorical dataset.
  *
  * functionality works in following way.
  *
  *
  * <br> 1. Iterates over each Array of columnEntries where each columnEntry represents Vector
  * <br> 2. Generating histogram for vector from column Entry
  * <br> 3. Output map of featureID and histogram.
  *
  */
class NamedMatrixToFeaturedHistogram
  extends RichMapFunction[NamedMatrix, mutable.Map[String, Histogram]] {

  override def map(namedMatrixRep: NamedMatrix)
  : mutable.Map[String, Histogram] = {
    val featuredHistogram: mutable.Map[String, Histogram] =
      NamedMatrixToFeaturedHistogram(namedMatrixRep = namedMatrixRep)

    featuredHistogram
  }
}
