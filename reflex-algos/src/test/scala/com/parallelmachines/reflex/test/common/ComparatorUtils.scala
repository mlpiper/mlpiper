package com.parallelmachines.reflex.test.common

import breeze.linalg.{DenseMatrix, DenseVector}

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
}
