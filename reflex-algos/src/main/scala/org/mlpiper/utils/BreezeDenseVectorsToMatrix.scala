package org.mlpiper.utils

import breeze.linalg.{DenseMatrix, DenseVector}

object BreezeDenseVectorsToMatrix {

  /**
    * Converts Iterator of DenseVector to a BreezeDenseMatrix
    */
  def iteratorToMatrix(iterator: Iterator[DenseVector[Double]])
  : DenseMatrix[Double] = {
    val (a, b) = iterator.duplicate
    val (c, d) = a.duplicate
    val length = b.length
    val attributes = c.next().length

    val matrix = new DenseMatrix[Double](length, attributes)
    for (i <- 0 until length) {
      matrix(i, ::) := d.next().t
    }
    matrix
  }

  /**
    * Converts Iterable of DenseVector to a DenseMatrix
    */
  def iterableToMatrix(iterable: Iterable[DenseVector[Double]])
  : DenseMatrix[Double] = {
    iteratorToMatrix(iterable.toIterator)
  }
}
