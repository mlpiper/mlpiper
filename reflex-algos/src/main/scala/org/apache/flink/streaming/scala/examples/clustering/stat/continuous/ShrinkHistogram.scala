package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import breeze.linalg.DenseVector

import scala.collection.mutable.ListBuffer

/**
  * Object ShrinkHistogram is responsible for shrinking histogram according to require bin size
  */
object ShrinkHistogram {

  /**
    * method will shrink bin and histlist according to reduction requirement.
    * it will merge lowest bin with adjacent lowest bin.
    *
    * Algorithm will consider all possible scenarios of hist details
    * <br> 1. (4,3,2)
    * <br> need to drop last element and merge with second to left element
    * <br> need to merge minIndex and minIndex - 1
    * <br> mergeWithLeft will take minIndex
    *
    * <br> 2. (2,3,4)
    * <br> need to drop first element and merge first two elements
    * <br> need to merge minIndex and minIndex + 1
    * <br> mergeWithLeft will take minIndex + 1
    *
    * <br> 3. (3,2,4)
    * <br> need to drop second element and merge with left one
    * <br> need to merge minIndex and minIndex - 1
    * <br> mergeWithLeft will take minIndex
    *
    * <br> 4. (4,2,3)
    * <br> need to drop second element and merge with right one
    * <br> need to merge minIndex and minIndex + 1
    * <br> mergeWithLeft will take minIndex + 1
    */
  private def shrinkHist(binEdgeList: ListBuffer[Double],
                         histList: ListBuffer[Double],
                         reductionRequired: Int): Unit = {
    for (_ <- 0 until reductionRequired) {
      val minIndex = histList.indexOf(histList.min)
      var indexToDrop = minIndex

      val rightIndexToMin = minIndex + 1
      val leftIndexToMin = minIndex - 1
      val maxSize = histList.length

      if ((rightIndexToMin < maxSize) &&
        ((leftIndexToMin < 0) ||
          (histList(rightIndexToMin) < histList(leftIndexToMin)))) {
        indexToDrop += 1
      }
      mergeWithLeftBin(binEdgeList = binEdgeList, histList = histList, indexToDrop = indexToDrop)
    }
  }

  /**
    * method will first merge hist details of index and left index value
    * and then drop index from bin and histlist
    */
  private def mergeWithLeftBin(binEdgeList: ListBuffer[Double],
                               histList: ListBuffer[Double],
                               indexToDrop: Int): Unit = {
    // add up index detail with left's hist
    histList(indexToDrop - 1) = histList(indexToDrop - 1) + histList(indexToDrop)

    // drop index from both bin and histList
    binEdgeList.remove(indexToDrop)
    histList.remove(indexToDrop)
  }

  /**
    * method shrinks histogram according to require bin size
    */
  def reduce(hist: Histogram, shrinkSize: Int)
  : Histogram = {
    val binEdgeList: ListBuffer[Double] = hist.binEdges.toArray.to[ListBuffer]
    val histList: ListBuffer[Double] = hist.hist.toArray.to[ListBuffer]
    val reductionRequired = binEdgeList.length - shrinkSize

    shrinkHist(binEdgeList = binEdgeList, histList = histList, reductionRequired = reductionRequired)

    new Histogram(histVector = DenseVector[Double](histList.toArray), binEdgesVector = DenseVector[Double](binEdgeList.toArray))
  }
}
