package org.mlpiper.stat.algos.svm

import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator

class SVMToMarginWidthStatForSpark(weights: org.apache.spark.mllib.linalg.Vector,
                                   sparkContext: org.apache.spark.SparkContext) {

  private var globalStat: GlobalAccumulator[SVMModelMarginWidth] = _

  def updateStatAccumulator(modelMargin: SVMModelMarginWidth): Unit = {
    if (globalStat != null) {
      globalStat.localUpdate(modelMargin)
    } else {
      globalStat = SVMModelMarginWidth.getAccumulator(modelMargin)
    }
    globalStat.updateSparkAccumulator(this.sparkContext)
  }

  def generateModelStat(): Unit = {
    val marginWidth = SVMModelMarginWidth.calculateMarginWidth(weights)

    this.updateStatAccumulator(modelMargin = marginWidth)
  }
}
