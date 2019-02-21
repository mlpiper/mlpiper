package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import breeze.linalg.{DenseMatrix, DenseVector}
import com.parallelmachines.reflex.common.InfoType
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.scala.examples.common.stats._

import scala.collection.mutable

@deprecated
object DenseMatrixToFeaturedHistogram {

  val DefaultBinSize = 10

  def getAccumulator(accumName: String, startingHist: HistogramWrapper)
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
          infoType = x.infoType)
      },
      startingValue = startingHist,
      accumDataType = AccumData.getGraphType(startingHist.histogram),
      accumModeType = AccumMode.Instant,
      infoType = InfoType.InfoType.General
    )
  }

  /** Method is responsible for generating Min and Max bin range from given featuredHistogram.
    * Method will also create bin size requirement for each features.
    * Map representation has integer key and it will always start with 0.
    */
  def generateMinMaxBinRangeAndBinSizeFromFeaturedHist(featuredHistogram: mutable.Map[String, Histogram],
                                                       excludeGaurdedBin: Boolean = true)
  : (Array[Double], Array[Double], Array[Int]) = {
    val guardedBins: Int = if (excludeGaurdedBin) 1 else 0

    // min and max list will be for each feature. So creating list of size of arrayOfBinEdgesOfEachFeature
    val minBinList: Array[Double] = new Array[Double](featuredHistogram.size)
    val maxBinList: Array[Double] = new Array[Double](featuredHistogram.size)

    val binSizeOfEachFeature: Array[Int] = new Array[Int](featuredHistogram.size)

    val mapOfFeatureAndBin: mutable.Map[String, DenseVector[Double]] = featuredHistogram.map(x => (x._1, x._2.binEdges))

    //  filling up min and max bin list
    mapOfFeatureAndBin.foreach(x => {
      minBinList(x._1.toInt) = x._2(0 + guardedBins)
      maxBinList(x._1.toInt) = x._2(x._2.length - 1 - guardedBins)
      binSizeOfEachFeature(x._1.toInt) = x._2.length - 1 - (2 * guardedBins)
    })

    (minBinList, maxBinList, binSizeOfEachFeature)
  }

  /**
    * Method/API is responsible for creating Featured Histogram from matrix for given bin ranges of each attributes.
    */
  @deprecated
  def apply(matrix: DenseMatrix[Double],
            binSizeForEachFeatureForRef: Option[Array[Int]] = None,
            minBinValues: Option[Array[Double]],
            maxBinValues: Option[Array[Double]]): mutable.Map[String, Histogram] = {

    val binSizeForEachFeature: Array[Int] =
      if (binSizeForEachFeatureForRef.isDefined) binSizeForEachFeatureForRef.get
      else Array.fill(matrix.cols)(DenseMatrixToFeaturedHistogram.DefaultBinSize)

    // tranposing matrix
    val transpose = matrix.t

    // map of featureID and histogram
    val mapOfFeatureIDAndHist = mutable.Map[String, Histogram]()

    for (eachFeature <- 0 until transpose.rows) {
      // innerVector is vector for relevant row
      val innerVector = transpose(eachFeature, ::).inner

      // selecting min range if provided
      val lowerBound: Option[Double] = if (minBinValues.isDefined) {
        Some(minBinValues.get(eachFeature))
      } else {
        None
      }
      // selecting max range if provided
      val upperBound: Option[Double] = if (maxBinValues.isDefined) {
        Some(maxBinValues.get(eachFeature))
      } else {
        None
      }

      // calculate histogram of given DenseVector
      val vectorHistogram = FeaturedHistogramFromDenseVector
        .getHistogram(vector = innerVector,
          binSize = Some(binSizeForEachFeature(eachFeature)),
          lowerBound = lowerBound,
          upperBound = upperBound)

      if (vectorHistogram.isDefined) {
        mapOfFeatureIDAndHist += (eachFeature.toString -> vectorHistogram.get)
      }
    }

    mapOfFeatureIDAndHist
  }
}

/**
  * Compute histogram on DataStream of DenseMatrix[Double]
  *
  * functionality works in following way.
  *
  *
  * <br> 1. Transpose the Dense Matrix so that all feature values combined into single row
  * <br> 2. Iterate over transpose matrix and calculate hist
  * <br> 3. Output map of featureID and histogram matrix.
  *
  * @note This class is soon to be deprecated
  * @param minBinValues Array specifying least values in bin
  * @param maxBinValues Array specifying max values in bin
  */
@deprecated
class DenseMatrixToFeaturedHistogram(binSizeForEachFeatureForRef: Option[Array[Int]] = None,
                                     minBinValues: Option[Array[Double]],
                                     maxBinValues: Option[Array[Double]])
  extends RichMapFunction[DenseMatrix[Double], mutable.Map[String, Histogram]] {

  override def map(matrix: DenseMatrix[Double]): mutable.Map[String, Histogram] = {
    val featuredHistogram: mutable.Map[String, Histogram] =
      DenseMatrixToFeaturedHistogram(matrix = matrix,
        binSizeForEachFeatureForRef = binSizeForEachFeatureForRef,
        minBinValues = minBinValues,
        maxBinValues = maxBinValues)

    featuredHistogram
  }
}