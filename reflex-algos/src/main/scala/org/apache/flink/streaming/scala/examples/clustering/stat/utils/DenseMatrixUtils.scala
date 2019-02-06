package org.apache.flink.streaming.scala.examples.clustering.stat.utils

import breeze.linalg.DenseMatrix

import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.{DataStream, _}

/**
  * Object DenseMatrixUtils is responsible for providing functionality to compute distance matrix representation.
  */
object DenseMatrixUtils {

  /**
    * Compute the distance matrix for the input DataSet.
    * It will generate distance matrix using [[DistanceMatrixGenerator]]
    *
    * @param set DataSet of DenseMatrix[Double].
    * @return A distance matrix representing distance between each row in row by row representation.
    */
  def createDistanceMatrix(set: DataSet[DenseMatrix[Double]])
  : DataSet[DenseMatrix[Double]] = {
    val distanceMatrixGenerator = new DistanceMatrixGenerator
    set.flatMap(distanceMatrixGenerator)
  }

  /**
    * Compute the distance matrix for the input DataSet.
    * It will generate distance matrix using [[DistanceMatrixGenerator]]
    *
    * @param stream DataStream of DenseMatrix[Double].
    * @return A distance matrix representing distance between each row in row by row representation.
    */
  def createDistanceMatrix(stream: DataStream[DenseMatrix[Double]])
  : DataStream[DenseMatrix[Double]] = {
    val distanceMatrixGenerator = new DistanceMatrixGenerator
    stream.flatMap(distanceMatrixGenerator)
  }
}

