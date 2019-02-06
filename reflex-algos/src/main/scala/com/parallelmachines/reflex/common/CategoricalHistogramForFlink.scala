package com.parallelmachines.reflex.common

import breeze.linalg.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexNamedMatrix, ReflexNamedVector}
import org.apache.flink.streaming.scala.examples.clustering.stat.HistogramComparatorTypes
import org.apache.flink.streaming.scala.examples.clustering.stat.categorical.{Histogram => CategoricalHistogram, HistogramWrapper => CategoricalHistogramWrapper, OverlapResult => OverlapResultForCategoricalFeature}

import scala.collection.mutable

class CategoricalHistogramForFlink(healthName: String) extends HealthComponentFlink {
  var streamOfDenseVector: DataStream[DenseVector[Double]] = _
  var windowingSize: Long = _
  var enableCombining: Boolean = true
  var enableAccumOutputOfHistograms: Boolean = true
  var streamOfReflexNamedVector: DataStream[ReflexNamedVector] = _
  var contenderHistStream: DataStream[mutable.Map[String, CategoricalHistogram]] = _
  var overlapType: String = HistogramComparatorTypes.ProbabilityDistribution.toString
  var addAdjustmentNormalizingEdge: Boolean = true

  override def setIncomingHealthStream(input: DataStream[String]): Unit = {
    contenderHistStream = input.map(x => CategoricalHistogramWrapper.fromString(x))
  }

  override def generateHealth(): Unit = {
    /* Do Nothing*/
  }

  override def generateHealthAndCompare(): Unit = {
    val _ = createHealthAndCompare()
  }

  override def getName(): String = {
    this.healthName
  }

  def createHealthAndCompare(): DataStream[OverlapResultForCategoricalFeature] = {
    require(streamOfReflexNamedVector != null)
    require(contenderHistStream != null)

    val streamOfReflexCategoricalNamedVector = streamOfReflexNamedVector.map(_.toCategoricalNamedVector(dropNa = true))

    val streamOfMatrixes: DataStream[ReflexNamedMatrix] =
      GenericNamedMatrixUtils.createReflexNamedMatrix(dataStreamOfNamedVector = streamOfReflexCategoricalNamedVector, windowingSize = windowingSize)

    // calling up health creation for categorical features
    new HistogramForCategoricalFeatures(accumName = healthName)
      .createHistogramAndCompare(streamOfMatrixes = streamOfMatrixes,
        contenderHistStream = contenderHistStream,
        enableAccumOutputOfHistograms = enableAccumOutputOfHistograms,
        method = HistogramComparatorTypes.withName(overlapType),
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge,
        modelId = this.getModelId().getOrElse(null))
  }
}
