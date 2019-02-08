package org.mlpiper.stat.algos.kmeans

import breeze.linalg.DenseMatrix
import breeze.linalg.functions.euclideanDistance

/**
  * object will generate Distance Matrix from given Dense Matrix.
  * Distance matrix will represent distances between two rows of Dense Matrix.
  */
object DistanceMatrixGenerator {
  def apply(denseMatrix: DenseMatrix[Double])
  : DenseMatrix[Double] = {
    val rows = denseMatrix.rows

    // distance matrix is going to be size of row by row matrix
    val distanceMatrix: DenseMatrix[Double] = DenseMatrix.zeros[Double](rows = rows, cols = rows)

    // filling up distance matrix by considering euclidean distance between two rows of given dense matrix
    for (i <- 0 until rows) {
      val iDenseVector = denseMatrix(i, ::).inner

      for (j <- i + 1 until rows) {
        val jDenseVector = denseMatrix(j, ::).inner

        val distanceBetweeniAndj = euclideanDistance(iDenseVector, jDenseVector)

        distanceMatrix(i, j) = distanceBetweeniAndj
        distanceMatrix(j, i) = distanceBetweeniAndj
      }
    }
    distanceMatrix
  }
}
