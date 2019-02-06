package org.apache.flink.streaming.scala.examples.clustering.stat.utils

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.flink.streaming.scala.examples.functions.math.matrix.BreezeDenseVectorsToMatrix
import org.apache.spark

/**
  * Object is responsible for providing functionality to convert array of spark's Vector representation to DenseMatrix
  */
object ArrayVectorToDenseMatrix {
  def apply(arrayVector: Array[spark.mllib.linalg.Vector])
  : DenseMatrix[Double] = {
    val arrayOfDenseVectors = arrayVector.map(_.toArray).map(DenseVector(_))

    BreezeDenseVectorsToMatrix.iteratorToMatrix(iterator = arrayOfDenseVectors.toIterator)
  }

  def apply(arrayVector: Array[spark.ml.linalg.Vector])
  : DenseMatrix[Double] = {
    val arrayOfDenseVectors = arrayVector.map(_.toArray).map(DenseVector(_))

    BreezeDenseVectorsToMatrix.iteratorToMatrix(iterator = arrayOfDenseVectors.toIterator)
  }
}
