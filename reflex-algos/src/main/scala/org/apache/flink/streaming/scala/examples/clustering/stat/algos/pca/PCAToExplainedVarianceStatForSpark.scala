package org.apache.flink.streaming.scala.examples.clustering.stat.algos.pca

import breeze.linalg.{DenseVector => BreezeDenseVector}
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator

/**
  * class is responsible for providing functionality to generate and update distance matrix stat of centroid for Spark.
  */
class PCAToExplainedVarianceStatForSpark(explainedVariance: BreezeDenseVector[Double],
                                         sparkContext: org.apache.spark.SparkContext) {

  private var globalStat: GlobalAccumulator[ExplainedVarianceStat] = _

  def updateStatAccumulator(explainedVarianceWrapper: ExplainedVarianceStat): Unit = {
    if (globalStat != null) {
      globalStat.localUpdate(explainedVarianceWrapper)
    } else {
      globalStat = ExplainedVarianceStat.getAccumulator(explainedVarianceWrapper)
    }
    globalStat.updateSparkAccumulator(this.sparkContext)
  }

  def generateExplainedVarianceStat(): Unit = {
    val pcaExplainedVarianceWrapper = ExplainedVarianceStat(explainedVariance = this.explainedVariance)

    this.updateStatAccumulator(explainedVarianceWrapper = pcaExplainedVarianceWrapper)
  }
}
