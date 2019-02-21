package com.parallelmachines.reflex.common


import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.scala.examples.clustering.math.ReflexNamedMatrix
import org.apache.flink.streaming.scala.examples.clustering.stat.HistogramComparatorTypes
import org.apache.flink.streaming.scala.examples.clustering.stat.continuous.{Histogram => ContinuousHistogram, HistogramWrapper => ContinuousHistogramWrapper, OverlapResult => OverlapResultForContinuousFeature, _}
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class HistogramForContinuousFeatures(accumName: String)
  extends Serializable {

  //////////////////////////////////////////////////////////////////////////////////////
  // Flink - Streaming Applications
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Class is responsible for updating accumulator for featured histogram in flink.
    */
  private class HistogramFlinkAccumulatorUpdater(infoType: InfoType, modelId: String, enableAccumOutputOfHistograms: Boolean = true)
    extends RichMapFunction[mutable.Map[String, ContinuousHistogram], mutable.Map[String, ContinuousHistogram]] {

    private var globalHistStat: GlobalAccumulator[ContinuousHistogramWrapper] = _

    def updateStatAccumulator(hist: mutable.Map[String, ContinuousHistogram]): Unit = {
      if (globalHistStat != null) {
        globalHistStat.localUpdate(ContinuousHistogramWrapper(hist))
      } else {
        globalHistStat = NamedMatrixToFeaturedHistogram.getAccumulator(accumName, infoType, modelId, ContinuousHistogramWrapper(hist))
      }
      globalHistStat.updateFlinkAccumulator(this.getRuntimeContext)
    }

    override def map(value: mutable.Map[String, ContinuousHistogram])
    : mutable.Map[String, ContinuousHistogram] = {
      if (enableAccumOutputOfHistograms) {
        this.updateStatAccumulator(value)
      }

      value
    }
  }

  /**
    * createHistogram function is responsible for calculating histogram on Stream of NamedVector
    *
    * @param streamOfMatrix                DataStream of ReflexNamedMatrix
    * @param windowingSize                 Size of window for which Histogram needs to be created
    * @param binSizeForEachFeatureForRef   Array specifying size requirement for each bins
    * @param minBinValues                  Array specifying least values in bin
    * @param maxBinValues                  Array specifying max values in bin
    * @param enableCombining               Parameter to enable/disable combining all histograms generated on parallel subtasks
    * @param enableAccumOutputOfHistograms Parameter to enable/disabling outputting histogram to accumulator
    * @return A map containing the featureID and histogram object
    */
  def createHistogram(streamOfMatrix: DataStream[ReflexNamedMatrix],
                      windowingSize: Long,
                      binSizeForEachFeatureForRef: Option[Map[String, Double]],
                      minBinValues: Option[Map[String, Double]],
                      maxBinValues: Option[Map[String, Double]],
                      enableCombining: Boolean,
                      enableAccumOutputOfHistograms: Boolean,
                      infoType: InfoType,
                      modelId: String)
  : DataStream[mutable.Map[String, ContinuousHistogram]] = {

    // generating histogram for each subtask's denseMatrix
    val eachSubtaskHist: DataStream[mutable.Map[String, ContinuousHistogram]] =
      streamOfMatrix
        .map(new NamedMatrixToFeaturedHistogram(
          binSizeForEachFeatureForRef = binSizeForEachFeatureForRef,
          minBinValueForEachFeatureForRef = minBinValues,
          maxBinValueForEachFeatureForRef = maxBinValues))

    val finalHistogramStream: DataStream[mutable.Map[String, ContinuousHistogram]] =
    // combining histograms of all subtask if functionality is enabled
      if (enableCombining) {
        val parallelism = eachSubtaskHist.javaStream.getParallelism

        eachSubtaskHist
          .countWindowAll(parallelism)
          .reduce(new CombineFeaturedHistograms)
      }
      else {
        eachSubtaskHist
      }

    // updating accumulator if it is enabled
    finalHistogramStream
      .map(new HistogramFlinkAccumulatorUpdater(infoType, modelId, enableAccumOutputOfHistograms = enableAccumOutputOfHistograms))
  }

  /**
    * Method compares input stream of DenseVector with contender Histogram Stream
    *
    * @param streamOfMatrixes              DataStream of input named matrix.
    * @param windowingSize                 Size of count window that will be placed on input stream to create histogram.
    * @param contenderHistStream           DataStream of featured histograms that will be use as reference to compare with input hist.
    * @param enableAccumOutputOfHistograms Flag to enable outputting generated histogram to accumulators
    * @param overlapType                   Overlap type, i.e. rmse
    * @return Overlap score based on overlap method provided
    */
  def createHistogramAndCompare(streamOfMatrixes: DataStream[ReflexNamedMatrix],
                                windowingSize: Long,
                                contenderHistStream: DataStream[mutable.Map[String, ContinuousHistogram]],
                                enableAccumOutputOfHistograms: Boolean,
                                overlapType: String,
                                addAdjustmentNormalizingEdge: Boolean,
                                modelId: String)
  : DataStream[OverlapResultForContinuousFeature] = {

    // broadcasting contender data to all subtask
    val broadcastedContenderHistStream = contenderHistStream.broadcast

    // connecting two inputHistStream and contenderHistStream
    val connectedStreams = broadcastedContenderHistStream.connect(streamOfMatrixes)

    // calculating score
    connectedStreams
      .flatMap(new GenerateHistogramAndCalculateOverlap(accumName, overlapType = overlapType, outputGeneratedHistogramToAccum = enableAccumOutputOfHistograms, addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge, modelId = modelId))
      // TODO: Handle multiple nodes flatMap order properly instead of generating histogram on single nodes
      .setParallelism(1)
  }

  /**
    * Method compares input Histogram Stream with contender Histogram Stream
    *
    * @param joinedHistStream                       Joined DataStream of featured histograms that will be use as reference to compare with input hist.
    * @param overlapType                            Overlap type, i.e. rmse
    * @param enableAccumOutputOfHistogramsWithScore enable/disable outputting histograms along with score in accumulator. (flag is useful for canary)
    * @return Overlap score based on overlap method provided
    */
  def compareHistogram(joinedHistStream: DataStream[(mutable.Map[String, ContinuousHistogram], mutable.Map[String, ContinuousHistogram])],
                       overlapType: String,
                       enableAccumOutputOfHistogramsWithScore: Boolean,
                       addAdjustmentNormalizingEdge: Boolean,
                       modelId: String)
  : DataStream[OverlapResultForContinuousFeature] = {
    // calculating score
    joinedHistStream.map(new CalculateOverlapScore(overlapType = overlapType, enableAccumOutputOfHistogramsWithScore = enableAccumOutputOfHistogramsWithScore, addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge, modelId = modelId))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // Spark - Batch Applications
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Class is responsible for updating accumulator for featured histogram in Spark.
    */
  private class HistogramSparkAccumulatorUpdater(infoType: InfoType, modelId: String, sc: SparkContext) {

    private var globalHistStat: GlobalAccumulator[ContinuousHistogramWrapper] = _

    def updateStatAccumulator(hist: mutable.Map[String, ContinuousHistogram]): Unit = {
      if (globalHistStat != null) {
        globalHistStat.localUpdate(ContinuousHistogramWrapper(hist))
      } else {
        globalHistStat = NamedMatrixToFeaturedHistogram.getAccumulator(accumName, infoType, modelId, ContinuousHistogramWrapper(hist))
      }
      globalHistStat.updateSparkAccumulator(sc)
    }
  }

  /**
    * createHistogram function is responsible for calculating histogram on RDD of namedVector
    * Method gives access to named histogram generation on RDD.
    *
    * @param rddOfNamedMatrix                RDD of NamedMatrix
    * @param binSizeForEachFeatureForRef     Map specifying size requirement for each bins associated with each featureIDs
    * @param minBinValueForEachFeatureForRef Map specifying least values in bin associated with each featureIDs
    * @param maxBinValueForEachFeatureForRef Map specifying max values in bin associated with each featureIDs
    * @return A map containing the featureID and histogram object
    */
  def createHistogram(rddOfNamedMatrix: RDD[ReflexNamedMatrix],
                      binSizeForEachFeatureForRef: Option[Map[String, Double]],
                      minBinValueForEachFeatureForRef: Option[Map[String, Double]],
                      maxBinValueForEachFeatureForRef: Option[Map[String, Double]],
                      enableAccumOutputOfHistograms: Boolean,
                      scOption: Option[SparkContext],
                      infoType: InfoType,
                      modelId: String)
  : mutable.Map[String, ContinuousHistogram] = {


    val histogram = rddOfNamedMatrix
      // Convert each matrix into a histogram
      .map(x => NamedMatrixToFeaturedHistogram(x,
      binSizeForEachFeatureForRef = binSizeForEachFeatureForRef,
      minBinValueForEachFeatureForRef = minBinValueForEachFeatureForRef,
      maxBinValueForEachFeatureForRef = maxBinValueForEachFeatureForRef))
      // Merge each partition's histogram using a reduce
      .reduce((x, y) => CombineFeaturedHistograms.combineTwoFeaturedHistograms(x, y))

    // updating accumulator if it is enabled
    if (enableAccumOutputOfHistograms) {
      new HistogramSparkAccumulatorUpdater(infoType, modelId, sc = scOption.get).updateStatAccumulator(histogram)
    }

    histogram
  }

  def compareHistogram(contenderHistogram: mutable.Map[String, ContinuousHistogram],
                       inferenceHist: mutable.Map[String, ContinuousHistogram],
                       method: HistogramComparatorTypes.HistogramComparatorMethodType,
                       addAdjustmentNormalizingEdge: Boolean,
                       sc: SparkContext,
                       modelId: String): OverlapResultForContinuousFeature = {

    val score = CompareTwoFeaturedHistograms
      .compare(contenderfeaturedHistogram = contenderHistogram,
        inferencefeaturedHistogram = inferenceHist,
        method = method,
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

    val inputHistogramRepresentation = contenderHistogram
    val contenderHistogramRepresentation = inferenceHist

    val overlapResult = new OverlapResultForContinuousFeature(score = score,
      inputHistStream = inputHistogramRepresentation,
      contenderHistStream = contenderHistogramRepresentation)

    val healthAcc = OverlapResultForContinuousFeature.getAccumulator(overlapResult, InfoType.InfoType.HealthCompare, modelId)
    healthAcc.updateSparkAccumulator(sc)

    overlapResult
  }
}
