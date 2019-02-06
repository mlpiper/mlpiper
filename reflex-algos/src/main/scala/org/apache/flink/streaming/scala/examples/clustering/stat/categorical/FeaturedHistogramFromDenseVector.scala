package org.apache.flink.streaming.scala.examples.clustering.stat.categorical

import breeze.linalg.DenseVector

import scala.collection.mutable

object FeaturedHistogramFromDenseVector {
  def getHistogramFromArrayOfString(vectorArrayString: Array[String]): Histogram = {
    val mapOfBinAndEdgeRep: mutable.Map[String, Double] = mutable.Map[String, Double]()

    vectorArrayString.foreach(eachVal => {
      val binVal = mapOfBinAndEdgeRep.getOrElseUpdate(eachVal, 0.0)
      mapOfBinAndEdgeRep.update(eachVal, binVal + 1)
    })

    new Histogram(categoricalCounts = mapOfBinAndEdgeRep.map(x => (x._1, x._2)).toMap)
  }

  /**
    * getHistogram function is responsible for creating histogram for given vector and required bin size
    *
    * @param vector Input vector
    * @return [[Histogram]] representation of input vector
    */
  def getHistogram(vector: DenseVector[_]): Option[Histogram] = {
    // From Any to cast as Array[Double], active iterator was needed !
    // Right now, only histogram from doubles is supported

    if (vector.length > 0) {
      val vectorArray: Array[String] = vector(0) match {
        case _: Double =>
          vector.asInstanceOf[DenseVector[Double]].activeValuesIterator.toArray.map(_.toString)
        case _: Int =>
          vector.asInstanceOf[DenseVector[Int]].activeValuesIterator.toArray.map(_.toString)
        case _: Long =>
          vector.asInstanceOf[DenseVector[Long]].activeValuesIterator.toArray.map(_.toString)
        case _: Float =>
          vector.asInstanceOf[DenseVector[Float]].activeValuesIterator.toArray.map(_.toString)
        case _: String =>
          vector.asInstanceOf[DenseVector[String]].activeValuesIterator.toArray
        case _ =>
          // giving it last try
          try {
            vector.toArray.map(_.toString)
          } catch {
            case exception: Exception =>
              exception.printStackTrace()
              Array.empty
          }
      }

      if (vectorArray.nonEmpty) {
        val histogram = getHistogramFromArrayOfString(vectorArrayString = vectorArray)
        Some(histogram)
      }
      else {
        None
      }
    }
    else {
      None
    }
  }
}
