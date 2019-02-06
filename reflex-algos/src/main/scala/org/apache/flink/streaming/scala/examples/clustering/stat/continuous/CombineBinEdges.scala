package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import breeze.linalg.DenseVector

import scala.collection.mutable.ListBuffer

/**
  * Object CombineBinEdges is responsible for combining two bin edges.
  * Merging will be done in using mergeSort's implementation
  */
object CombineBinEdges {
  def reduce(hist1: Histogram, hist2: Histogram)
  : DenseVector[Double] = {
    val reducedEdge: ListBuffer[Double] = new ListBuffer[Double]()

    val binEdge1 = hist1.binEdges
    val binEdge2 = hist2.binEdges

    val lengthBinEdge1 = binEdge1.length
    val lengthBinEdge2 = binEdge2.length

    var indexBinEdge1 = 0
    var indexBinEdge2 = 0

    while (indexBinEdge1 < lengthBinEdge1 && indexBinEdge2 < lengthBinEdge2) {
      if (binEdge1(indexBinEdge1) < binEdge2(indexBinEdge2)) {
        reducedEdge += binEdge1(indexBinEdge1)
        indexBinEdge1 += 1
      }
      else if (binEdge1(indexBinEdge1) == binEdge2(indexBinEdge2)) {
        reducedEdge += binEdge1(indexBinEdge1)
        indexBinEdge1 += 1
        indexBinEdge2 += 1
      }
      else {
        reducedEdge += binEdge2(indexBinEdge2)
        indexBinEdge2 += 1
      }
    }

    while (indexBinEdge1 < lengthBinEdge1) {
      reducedEdge += binEdge1(indexBinEdge1)
      indexBinEdge1 += 1
    }

    while (indexBinEdge2 < lengthBinEdge2) {
      reducedEdge += binEdge2(indexBinEdge2)
      indexBinEdge2 += 1
    }

    new DenseVector[Double](reducedEdge.toArray)
  }
}
