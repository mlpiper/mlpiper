package org.apache.flink.streaming.scala.examples.clustering.stat.utils

import org.apache.flink.streaming.scala.examples.clustering.stat.algos.kmeans.DistanceMatrixStat

object DistanceMatrixStatGenerator {
  /**
    * Compute the distance statistic for the input DenseMatrix.
    *
    * @param centroid Centroids representation for KMeans in Flink
    * @return A distance matrix statistics representing DistanceMatrix, MeanVector and VarianceVector.
    */
  def apply(centroid: org.apache.flink.streaming.scala.examples.clustering.kmeans2.model.Centroids)
  : DistanceMatrixStat = {
    val distanceMatrix = DistanceMatrixGenerator(denseMatrix = centroid.centers)

    // distanceMatrixStat will hold the DistanceMatrixStat
    val distanceMatrixStat = DistanceMatrixStat(distanceMatrix = distanceMatrix)

    // # of points classified in each bucket
    if (centroid.weights != null) {
      distanceMatrixStat.countVector = Some(centroid.weights)
    }

    if (centroid.distanceMean != null) {
      distanceMatrixStat.meanVector = Some(centroid.distanceMean)
    }

    if (centroid.distanceVar != null) {
      distanceMatrixStat.varianceVector = Some(centroid.distanceVar)
    }

    distanceMatrixStat
  }

  /**
    * Compute the distance statistic for the input DenseMatrix.
    *
    * @param centroid Models representation for KMeans in Spark
    * @return A distance matrix statistics representing DistanceMatrix, MeanVector and VarianceVector.
    */
  def apply(centroid: org.apache.spark.mllib.clustering.KMeansModel)
  : DistanceMatrixStat = {
    val centroidsDenseMatrix = ArrayVectorToDenseMatrix(centroid.clusterCenters)

    val distanceMatrix = DistanceMatrixGenerator(denseMatrix = centroidsDenseMatrix)

    // distanceMatrixStat will hold the DistanceMatrixStat
    val distanceMatrixStat = DistanceMatrixStat(distanceMatrix = distanceMatrix)

    distanceMatrixStat
  }

  /**
    * Compute the distance statistic for the input DenseMatrix.
    *
    * @param centroid Models representation for KMeans in Spark
    * @return A distance matrix statistics representing DistanceMatrix, MeanVector and VarianceVector.
    */
  def apply(centroid: org.apache.spark.ml.clustering.KMeansModel)
  : DistanceMatrixStat = {
    val centroidsDenseMatrix = ArrayVectorToDenseMatrix(centroid.clusterCenters)

    val distanceMatrix = DistanceMatrixGenerator(denseMatrix = centroidsDenseMatrix)

    // distanceMatrixStat will hold the DistanceMatrixStat
    val distanceMatrixStat = DistanceMatrixStat(distanceMatrix = distanceMatrix)

    distanceMatrixStat
  }
}
