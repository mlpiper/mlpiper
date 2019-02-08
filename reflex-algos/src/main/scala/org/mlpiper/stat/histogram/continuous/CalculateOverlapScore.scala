package org.mlpiper.stat.histogram.continuous

import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator
import org.mlpiper.stat.histogram.HistogramComparatorTypes

import scala.collection.mutable

/**
  * Compute Overlap on DataStreams of Joined Featured Histograms.
  * Class is not responsible for creating histogram, instead it will just generate score on joined histograms.
  *
  * @param overlapType                            Specifier of the methodology to use.
  * @param enableAccumOutputOfHistogramsWithScore Flag to output histograms as well score in accumulator (i.e. for canary)
  */
class CalculateOverlapScore(var overlapType: String,
                            modelId: String,
                            addAdjustmentNormalizingEdge: Boolean,
                            enableAccumOutputOfHistogramsWithScore: Boolean = false)
  extends RichMapFunction[(mutable.Map[String, Histogram], mutable.Map[String, Histogram]),
    OverlapResult] {


  private var globalStatForOverlapResult: GlobalAccumulator[OverlapResult] = _

  def updateStatAccumulator(overlapResult: OverlapResult): Unit = {
    if (globalStatForOverlapResult != null) {
      globalStatForOverlapResult.localUpdate(overlapResult)
    } else {
      globalStatForOverlapResult = OverlapResult.getAccumulator(overlapResult, InfoType.General, modelId)
    }
    globalStatForOverlapResult.updateFlinkAccumulator(this.getRuntimeContext)
  }

  override def map(value: (mutable.Map[String, Histogram], mutable.Map[String, Histogram]))
  : OverlapResult = {
    // calculating score using CompareTwoFeaturedHistograms
    val score: Map[String, Double] =
      CompareTwoFeaturedHistograms.compare(contenderfeaturedHistogram = value._1, inferencefeaturedHistogram = value._2, method = HistogramComparatorTypes.withName(overlapType), addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

    val inputStream1MLHealthRepresentation = value._1

    val inputStream2MLHealthRepresentation = value._2

    val overlapResult = new OverlapResult(
      score = score,
      inputHistStream = inputStream1MLHealthRepresentation,
      contenderHistStream = inputStream2MLHealthRepresentation,
      outputHistogramsAlongScore = enableAccumOutputOfHistogramsWithScore
    )

    this.updateStatAccumulator(overlapResult = overlapResult)

    overlapResult
  }
}
