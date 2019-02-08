package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import breeze.linalg.{DenseMatrix, DenseVector}
import org.mlpiper.utils.ParameterIndices

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object ComparatorUtils {

  def compareDouble(a: Double,
                    b: Double,
                    epsilon: Double)
  : Boolean = {
    Math.abs(a - b) < epsilon
  }

  def compareTwoMap(mapA: Map[String, Double], mapB: Map[String, Double]): Boolean = {

    val union = mapA.keys.toSet.union(mapB.keys.toSet)

    var keysAreSame = true
    mapA.keys.foreach(x => {
      keysAreSame = keysAreSame && union.contains(x)
    })
    mapB.keys.foreach(x => {
      keysAreSame = keysAreSame && union.contains(x)
    })

    var valuesAreSame = true
    if (keysAreSame) {
      mapA.keys.foreach(x => {
        valuesAreSame = valuesAreSame && compareDouble(mapA(x), mapB(x), 0.01)
      })
    }

    keysAreSame && valuesAreSame
  }

  /**
    * Compares two vectors' values by some epsilon.
    *
    * @param a       Vector a
    * @param b       Vector b
    * @param epsilon Epsilon to compare each element with
    * @return True if every element in a equals its respective element in b by epsilon.
    *         False otherwise.
    */
  def compareBreezeOutputVector(a: DenseVector[Double],
                                b: DenseVector[Double],
                                epsilon: Double = 0.01)
  : Boolean = {
    if (a.length != b.length) {
      return false
    }

    for (i <- 0 until a.length) {
      if (!compareDouble(a(i), b(i), epsilon)) {
        return false
      }
    }

    true
  }

  /**
    * Compares two dense matrix' values by some epsilon.
    *
    * @param a       Matrix a
    * @param b       Matrix b
    * @param epsilon Epsilon to compare each element with
    * @return True if every element in a equals its respective element in b by epsilon.
    *         False otherwise.
    */
  def compareBreezeOutputMatrix(a: DenseMatrix[Double],
                                b: DenseMatrix[Double],
                                epsilon: Double = 0.01)
  : Boolean = {
    if (a.rows != b.rows || a.cols != b.cols) {
      return false
    }

    for (i <- 0 until a.rows) {
      if (!compareBreezeOutputVector(a(i, ::).inner, b(i, ::).inner, epsilon)) {
        return false
      }
    }

    true
  }

  /**
    * Compares calculatedOutput vectors with expectedOutput vectors regardless of order.
    *
    * @param calculatedOutput Output
    * @param expectedOutput   Expected output that contains all elements.
    * @param indices          Indices to extract data from expectedOutput.
    * @return True all vectors in transformedOutput are same as expectedOutput. False otherwise.
    */
  def compareBreezeDenseVectorSeq(calculatedOutput: Seq[DenseVector[Double]],
                                  expectedOutput: Seq[DenseVector[Double]],
                                  indices: ParameterIndices)
  : Boolean = {
    if (calculatedOutput.length != expectedOutput.length) {
      return false
    }

    val calculatedOutputList = calculatedOutput.toList.to[ListBuffer]
    val expectedOutputList = expectedOutput.toList.to[ListBuffer]

    // Compare vector portions
    for (i <- calculatedOutputList.indices) {
      var valueFound = false
      breakable {
        for (j <- expectedOutputList.indices) {
          if (compareBreezeOutputVector(calculatedOutputList(i), expectedOutputList(j))) {
            valueFound = true
            break()
          }
        }
      }
      if (!valueFound) {
        return false
      }
    }

    true
  }

  /**
    * Compares calculatedOutput matrix with expectedOutput matrix regardless of order.
    *
    * @param calculatedOutput Output
    * @param expectedOutput   Expected output that contains all elements.
    * @return True all vectors in transformedOutput are same as expectedOutput. False otherwise.
    */
  def compareBreezeDenseMatrixSeq(calculatedOutput: Seq[DenseMatrix[Double]],
                                  expectedOutput: Seq[DenseMatrix[Double]])
  : Boolean = {
    if (calculatedOutput.length != expectedOutput.length) {
      return false
    }

    val calculatedOutputList = calculatedOutput.toList.to[ListBuffer]
    val expectedOutputList = expectedOutput.toList.to[ListBuffer]

    // Compare vector portions
    for (i <- calculatedOutputList.indices) {
      var valueFound = false
      breakable {
        for (j <- expectedOutputList.indices) {
          if (compareBreezeOutputMatrix(calculatedOutputList(i), expectedOutputList(j))) {
            valueFound = true
            break()
          }
        }
      }
      if (!valueFound) {
        return false
      }
    }

    true
  }
}
