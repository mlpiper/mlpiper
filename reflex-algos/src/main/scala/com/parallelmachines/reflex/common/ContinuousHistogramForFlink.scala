package com.parallelmachines.reflex.common

import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexNamedMatrix, ReflexNamedVector}
import org.apache.flink.streaming.scala.examples.clustering.stat.HistogramComparatorTypes
import org.apache.flink.streaming.scala.examples.clustering.stat.continuous.{Histogram => ContinuousHistogram, HistogramWrapper => ContinuousHistogramWrapper, OverlapResult => OverlapResultForContinuousFeature}

import scala.collection.mutable

class ContinuousHistogramForFlink(healthName: String) extends HealthComponentFlink {

  var windowingSize: Long = _
  var binSizeForEachFeatureForRef: Option[Map[String, Double]] = None
  var minBinValues: Option[Map[String, Double]] = None
  var maxBinValues: Option[Map[String, Double]] = None
  var enableCombining: Boolean = true
  var enableAccumOutputOfHistograms: Boolean = true
  var streamOfNamedVector: DataStream[ReflexNamedVector] = _
  var contenderHistStream: DataStream[mutable.Map[String, ContinuousHistogram]] = _
  var overlapType: String = HistogramComparatorTypes.ProbabilityDistribution.toString
  var addAdjustmentNormalizingEdge: Boolean = true

  override def getName(): String = healthName

  override def generateHealth(): Unit = {
    val _ = createHealth()
  }

  override def generateHealthAndCompare(): Unit = {
    val _ = createHealthAndCompare()
  }

  override def setIncomingHealthStream(input: DataStream[String]): Unit = {
    contenderHistStream = input.map(x => ContinuousHistogramWrapper.fromString(x))
  }

  /**
    * createHistogram function is responsible for calculating histogram on Stream of NamedVector
    *
    * @return A map containing the featureID and histogram object
    */
  def createHealth(): DataStream[mutable.Map[String, ContinuousHistogram]] = {
    require(streamOfNamedVector != null)

    streamOfNamedVector = streamOfNamedVector.map(_.toContinuousNamedVector(dropNa = true))

    // creating denseMatrix on each subta-sk
    val streamOfMatrix = GenericNamedMatrixUtils.createReflexNamedMatrix(
      streamOfNamedVector,
      windowingSize)

    // considering all features are continuous, calling up health creation for continuous features
    new HistogramForContinuousFeatures(healthName)
      .createHistogram(streamOfMatrix = streamOfMatrix,
        windowingSize = windowingSize,
        binSizeForEachFeatureForRef = binSizeForEachFeatureForRef,
        minBinValues = minBinValues,
        maxBinValues = maxBinValues,
        enableCombining = enableCombining,
        enableAccumOutputOfHistograms = enableAccumOutputOfHistograms,
        InfoType.InfoType.Health,
        modelId = this.getModelId().orNull)
  }

  /**
    * Method compares input stream of NamedVector with contender Histogram Stream
    *
    * @return Overlap score based on overlap method provided
    */
  def createHealthAndCompare(): DataStream[OverlapResultForContinuousFeature] = {
    require(streamOfNamedVector != null)
    require(contenderHistStream != null)
    require(overlapType != null)

    val streamOfReflexContinuousNamedVector = streamOfNamedVector.map(_.toContinuousNamedVector(dropNa = true))

    val streamOfMatrixes: DataStream[ReflexNamedMatrix] =
      GenericNamedMatrixUtils.createReflexNamedMatrix(dataStreamOfNamedVector = streamOfReflexContinuousNamedVector, windowingSize = windowingSize)

    // considering all features are continuous, calling up health creation for continuous features
    new HistogramForContinuousFeatures(healthName)
      .createHistogramAndCompare(streamOfMatrixes = streamOfMatrixes,
        windowingSize = windowingSize,
        contenderHistStream = contenderHistStream,
        enableAccumOutputOfHistograms = enableAccumOutputOfHistograms,
        overlapType = overlapType,
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge,
        modelId = this.getModelId().orNull)
  }


  /**
    * Method compares input Histogram Stream with contender Histogram Stream
    *
    * @return Overlap score based on overlap method provided
    */

  var joinedHistStream: DataStream[(mutable.Map[String, ContinuousHistogram], mutable.Map[String, ContinuousHistogram])] = _
  var enableAccumOutputOfHistogramsWithScore: Boolean = _

  def compareHealth(): DataStream[OverlapResultForContinuousFeature] = {
    require(joinedHistStream != null)
    require(overlapType != null)

    new HistogramForContinuousFeatures(healthName)
      .compareHistogram(joinedHistStream = joinedHistStream,
        overlapType = overlapType,
        enableAccumOutputOfHistogramsWithScore = enableAccumOutputOfHistogramsWithScore,
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge,
        modelId = this.getModelId().orNull)
  }
}
