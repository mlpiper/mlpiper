package org.apache.flink.streaming.scala.examples.clustering.stat.utils

import breeze.linalg.DenseMatrix
import breeze.linalg.functions.euclideanDistance
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

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

/**
  * class is responsible for providing flatMap functionality to generate distance matrix representation from given dense matrix
  */
class DistanceMatrixGenerator
  extends RichFlatMapFunction[DenseMatrix[Double], DenseMatrix[Double]] {

  override def flatMap(denseMatrix: DenseMatrix[Double],
                       out: Collector[DenseMatrix[Double]])
  : Unit = {
    out.collect(DistanceMatrixGenerator(denseMatrix))
  }
}


