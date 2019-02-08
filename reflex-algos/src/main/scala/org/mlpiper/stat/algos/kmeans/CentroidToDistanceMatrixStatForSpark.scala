package org.mlpiper.stat.algos.kmeans

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator

/**
  * class is responsible for providing functionality to generate and update distance matrix stat of centroid for Spark.
  */
class CentroidToDistanceMatrixStatForSpark(sparkContext: org.apache.spark.SparkContext) extends Serializable {

  private var distanceMatrixAccumulator: GlobalAccumulator[DenseMatrix[Double]] = _
  private var meansAccumulator: GlobalAccumulator[DenseVector[Double]] = _
  private var variancesAccumulator: GlobalAccumulator[DenseVector[Double]] = _
  private var countAccumulator: GlobalAccumulator[DenseVector[Double]] = _

  def updateStatAccumulator(distanceMatrixStat: DistanceMatrixStat): Unit = {
    if (distanceMatrixAccumulator != null) {
      distanceMatrixAccumulator.localUpdate(distanceMatrixStat.distanceMatrix)
    } else {
      distanceMatrixAccumulator = DistanceMatrixStat.getDistanceMatrixAccumulator(distanceMatrixStat)
    }
    distanceMatrixAccumulator.updateSparkAccumulator(sparkContext)

    if (distanceMatrixStat.meanVector.isDefined) {
      if (meansAccumulator != null) {
        meansAccumulator.localUpdate(
          distanceMatrixStat.meanVector.get)
      } else {
        meansAccumulator = DistanceMatrixStat.getMeansAccumulator(distanceMatrixStat)
      }
      meansAccumulator.updateSparkAccumulator(sparkContext)
    }

    if (distanceMatrixStat.varianceVector.isDefined) {
      if (variancesAccumulator != null) {
        variancesAccumulator.localUpdate(
          distanceMatrixStat.varianceVector.get)
      } else {
        variancesAccumulator = DistanceMatrixStat.getVariancesAccumulator(distanceMatrixStat)
      }
      variancesAccumulator.updateSparkAccumulator(sparkContext)
    }

    if (distanceMatrixStat.countVector.isDefined) {
      if (countAccumulator != null) {
        countAccumulator.localUpdate(
          distanceMatrixStat.countVector.get)
      } else {
        countAccumulator = DistanceMatrixStat.getCountsAccumulator(distanceMatrixStat)
      }
      countAccumulator.updateSparkAccumulator(sparkContext)
    }
  }

  def generateDistanceMatrixStat(model: org.apache.spark.ml.clustering.KMeansModel): Unit = {
    val distanceMatrixStat = DistanceMatrixStatGenerator(centroid = model)

    this.updateStatAccumulator(distanceMatrixStat = distanceMatrixStat)
  }
}


