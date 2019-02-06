package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import breeze.linalg.DenseVector

/**
  * Object [[FeaturedHistogramFromDenseVector]] is responsible for generating histogram from provided dense vector
  * Histograms will contain hist and binEdge details. Generation is done as following.
  *
  * <br> 1. Using fillBin functionality, it will create bin range with specified lower and upper bound. If bounds are not specified then lowest and highest value of feature will be consider as bound
  * <br> 2. Fill up the histEdgeList via iterating over vector by considering min-max range of bin.
  * <br> 3. By making sure that vector is not full of constant vector, method will populate bin range list
  * <br> 4. If vector contains only constant vector, then Histogram will be representation of one bin range only. (value +- 0.01)
  */
object FeaturedHistogramFromDenseVector {
  /**
    * Method is responsible for creating filling up hist list. Bin range is [-Infinity, lower bound ... upper bound, +Infinity].
    *
    * @param minBinValue Lower bound require in bin range
    * @param maxBinValue Upper bound require in bin range
    * @param binSize     Bin length
    * @param vectorArray vector array used to fill up hist list
    */
  private def fillHistList(minBinValue: Double,
                           maxBinValue: Double,
                           binSize: Double,
                           vectorArray: Array[Double]): Array[Double] = {
    // histlist will be size of bin + 2 as, data might fall in lower/upper guard bins
    val histListArray = new Array[Double](binSize.toInt + 2)

    // iterating over vector for filling up hist list
    vectorArray.map(x => {
      // deciding the bin index the value will fall in!
      var bin = (((x - minBinValue) / (maxBinValue - minBinValue)) * binSize).floor.toInt

      // if bin goes below zero, it probably belongs to range [-Infinity, lower bound]
      if (bin < 0) {
        bin = 0
      }
      // if bin goes above binSize, it probably belongs to range [upperBound, Infinity]
      else if (bin >= binSize) {
        bin = binSize.toInt + 1
      }
      // bin falls in range
      else {
        bin = bin + 1
      }
      // updating frequency in respective bins
      histListArray(bin) = histListArray(bin) + 1.0

      bin
    })

    histListArray
  }

  /**
    * Method is responsible for creating bins range list based on bin length and lower, upper bound.
    * Bin range will be as following
    * [-Infinity, lower bound ... upper bound, +Infinity]
    *
    * @param minBinValue Lower bound require in bin range
    * @param maxBinValue Upper bound require in bin range
    * @param binSize     Bin length
    */
  private def fillBinEdgeList(minBinValue: Double,
                              maxBinValue: Double,
                              binSize: Double): Array[Double] = {
    require(minBinValue < maxBinValue, s"Min bin value $minBinValue cannot be greater or equal to max bin value $maxBinValue")

    var finalMinBinValue = minBinValue
    var finalMaxBinValue = maxBinValue

    // setting values for constant with constant and constant + 0.01
    if (finalMinBinValue == finalMaxBinValue) {
      val middleValue = finalMinBinValue

      finalMinBinValue = middleValue - 0.01
      finalMaxBinValue = middleValue + 0.01
    }

    val step = (finalMaxBinValue - finalMinBinValue) * 1.0 / binSize

    val lowerGaurdBound = Int.MinValue.toDouble
    val upperGaurdBound = Int.MaxValue.toDouble

    // prepend and append guarded value
    val binListArray: Array[Double] = new Array[Double](binSize.toInt + 1)

    binListArray.zipWithIndex.foreach(x => binListArray(x._2) = finalMinBinValue + x._2 * step)

    val guardedBinListArray = lowerGaurdBound +: binListArray :+ upperGaurdBound

    guardedBinListArray
  }

  /** Method is used to decide lower bound, upper bound and bukcet size based on provided vector, and bound values
    * If vector only contains constants, then bin range will be between constant and constant + 0.01
    */
  def getMinMaxBinValueAndBinLengthFromBound(vector: Array[Double],
                                             binSize: Option[Double],
                                             lowerBound: Option[Double],
                                             upperBound: Option[Double]): (Double, Double, Double) = {
    val vectorNonNA = vector.filter(!_.isNaN)

    var minBinValue = Int.MinValue.toDouble
    var maxBinValue = Int.MaxValue.toDouble
    var binLength = -1.0

    // setting bin values if bound values are provided else it will be the lowest and highest values of provided array
    if (lowerBound.isEmpty || lowerBound.get.isNaN) {
      // values are found in N instead of NlogN
      if (!vectorNonNA.isEmpty) {
        minBinValue = vectorNonNA.reduce((x, y) => if (x < y) x else y)
      } else {
        minBinValue = Double.NaN
      }
    } else {
      minBinValue = lowerBound.get
    }

    if (upperBound.isEmpty || lowerBound.get.isNaN) {
      if (!vectorNonNA.isEmpty) {
        maxBinValue = vectorNonNA.reduce((x, y) => if (x > y) x else y)
      } else {
        minBinValue = Double.NaN
      }
    } else {
      maxBinValue = upperBound.get
    }

    binLength = NamedMatrixToFeaturedHistogram.DefaultBinSize

    // setting values for constant with constant and constant + 0.01
    // bin length will be set to 1
    if (minBinValue == maxBinValue) {
      binLength = 1.0

      val middleValue = minBinValue

      minBinValue = middleValue - 0.01
      maxBinValue = middleValue + 0.01
    }

    if (binSize.isDefined) {
      binLength = binSize.get
    }

    (minBinValue, maxBinValue, binLength)
  }

  def getHistogramFromArrayOfDoubles(vectorArrayDouble: Array[Double],
                                     binSize: Option[Double] = None,
                                     lowerBound: Option[Double] = None,
                                     upperBound: Option[Double] = None): Option[Histogram] = {
    val vectorArrayOfDoubleNonNA = vectorArrayDouble.filter(!_.isNaN)

    val (minBinValue, maxBinValue, binLength) = getMinMaxBinValueAndBinLengthFromBound(vector = vectorArrayOfDoubleNonNA, binSize = binSize, lowerBound = lowerBound, upperBound = upperBound)

    if (minBinValue.isNaN || maxBinValue.isNaN) {
      return None
    }

    require(minBinValue < maxBinValue, s"lower bound $minBinValue cannot be greater or equal to upper bound $maxBinValue")

    require(binLength > 0, s"Histogram cannot be generated from zero or negative numbers")

    val binListArray = fillBinEdgeList(minBinValue = minBinValue, maxBinValue = maxBinValue, binSize = binLength)
    val histListArray = fillHistList(minBinValue = minBinValue, maxBinValue = maxBinValue, binSize = binLength, vectorArray = vectorArrayOfDoubleNonNA)

    require(binListArray.length == (histListArray.length + 1),
      s"Bins And HistEdge are not in proper format!! Bin size was ${binListArray.length} whilst Hist size was ${histListArray.length}")

    val histogram: Histogram = new Histogram(histVector = DenseVector(histListArray), binEdgesVector = DenseVector(binListArray))

    Some(histogram)
  }

  /**
    * getHistogram function is responsible for creating histogram for given vector and required bin size
    *
    * @param vector     Input vector
    * @param binSize    Required bin size
    * @param lowerBound Lower bound required for histogram bin
    * @param upperBound Upper bound required for histogram bin
    * @return [[Histogram]] representation of input vector
    */
  def getHistogram(vector: DenseVector[_],
                   binSize: Option[Double] = None,
                   lowerBound: Option[Double] = None,
                   upperBound: Option[Double] = None): Option[Histogram] = {
    // From Any to cast as Array[Double], active iterator was needed !
    // Right now, only histogram from doubles is supported
    if (vector.length > 0) {
      vector(0) match {
        case _: Double =>
          val vectorArray = vector.asInstanceOf[DenseVector[Double]].activeValuesIterator.toArray

          val histogram = getHistogramFromArrayOfDoubles(vectorArrayDouble = vectorArray, binSize = binSize, lowerBound = lowerBound, upperBound = upperBound)
          histogram
        case _: Int =>
          val vectorArray = vector.asInstanceOf[DenseVector[Int]].activeValuesIterator.toArray.map(_.toDouble)

          val histogram = getHistogramFromArrayOfDoubles(vectorArrayDouble = vectorArray, binSize = binSize, lowerBound = lowerBound, upperBound = upperBound)
          histogram
        case _: Long =>
          val vectorArray = vector.asInstanceOf[DenseVector[Long]].activeValuesIterator.toArray.map(_.toDouble)

          val histogram = getHistogramFromArrayOfDoubles(vectorArrayDouble = vectorArray, binSize = binSize, lowerBound = lowerBound, upperBound = upperBound)
          histogram
        case _: Float =>
          val vectorArray = vector.asInstanceOf[DenseVector[Float]].activeValuesIterator.toArray.map(_.toDouble)

          val histogram = getHistogramFromArrayOfDoubles(vectorArrayDouble = vectorArray, binSize = binSize, lowerBound = lowerBound, upperBound = upperBound)
          histogram
        case _ =>
          try {
            // trying with strings last time
            val histogram = getHistogramFromArrayOfDoubles(vectorArrayDouble = vector.toArray.map(_.asInstanceOf[Double]), binSize = binSize, lowerBound = lowerBound, upperBound = upperBound)
            histogram
          } catch {
            case e: Exception =>
              e.printStackTrace()
              None
          }
      }
    } else {
      None
    }
  }
}
