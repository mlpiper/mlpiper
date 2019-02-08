package org.mlpiper.stat.algos.kmeans

import breeze.linalg.{DenseMatrix, DenseVector}
import org.mlpiper.utils.BreezeDenseVectorsToMatrix

object DistanceMatrixStatGenerator {
  /**
    * Compute the distance statistic for the input DenseMatrix.
    *
    * @param centroid Models representation for KMeans in Spark
    * @return A distance matrix statistics representing DistanceMatrix, MeanVector and VarianceVector.
    */
  def apply(centroid: org.apache.spark.ml.clustering.KMeansModel)
  : DistanceMatrixStat = {
    val centroidsArray: Array[DenseVector[Double]] = centroid.clusterCenters.map(_.toArray).map(DenseVector(_))

    val centroidsDenseMatrix: DenseMatrix[Double] = BreezeDenseVectorsToMatrix.iteratorToMatrix(iterator = centroidsArray.toIterator)

    val distanceMatrix = DistanceMatrixGenerator(denseMatrix = centroidsDenseMatrix)

    // distanceMatrixStat will hold the DistanceMatrixStat
    val distanceMatrixStat = DistanceMatrixStat(distanceMatrix = distanceMatrix)

    distanceMatrixStat
  }
}
