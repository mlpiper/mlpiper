package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.common.{GraphFormat, HealthConfEnum, HealthConfigurations}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable

/**
  * class represents Histogram of Doubles
  */
class Histogram(histVector: DenseVector[Double], binEdgesVector: DenseVector[Double]) extends Serializable with GraphFormat {
  private val numberPrecision: Int = HealthConfigurations.getConf(HealthConfEnum.NumberPrecision).asInstanceOf[Int]
  private val enableNumberPrecision: Boolean = HealthConfigurations.getConf(HealthConfEnum.EnableNumberPrecision).asInstanceOf[Boolean]

  var enableNormHist: Boolean = false

  def setEnableNormHist(value: Boolean): Histogram = {
    enableNormHist = value
    this
  }

  def hist: DenseVector[Double] =
    this.histVector

  def binEdges: DenseVector[Double] =
    this.binEdgesVector

  var outBinArray: Array[Double] = _
  var outHistArray: Array[Double] = _

  private def preFormat(): Unit = {
    outBinArray = this.binEdges.toArray
    outHistArray = this.hist.toArray

    if (enableNormHist) {
      val histSum = outHistArray.sum

      // normalizing histList
      outHistArray.zipWithIndex.foreach(
        eachHistElement =>
          outHistArray(eachHistElement._2) = eachHistElement._1 / histSum
      )
    }

    if (enableNumberPrecision) {
      // generating bin array in proper precision if flag is enabled
      outBinArray.zipWithIndex.foreach(
        eachBinElement => {
          outBinArray(eachBinElement._2) = BigDecimal(eachBinElement._1).setScale(numberPrecision, BigDecimal.RoundingMode.HALF_UP).toDouble
        }
      )

      // generating hist array in proper precision if flag is enabled
      outHistArray.zipWithIndex.foreach(
        eachHistElement => {
          outHistArray(eachHistElement._2) = BigDecimal.decimal(eachHistElement._1).setScale(numberPrecision, BigDecimal.RoundingMode.HALF_UP).toDouble
        }
      )
    }
  }

  def getDataMap: Map[String, Double] = {
    preFormat()
    val outMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
    for (i <- 1 until outBinArray.length) {
      val leftString =
        if (outBinArray(i - 1) == Int.MinValue.toDouble)
          Histogram.NegInfString
        else outBinArray(i - 1).toString
      val rightString =
        if (outBinArray(i) == Int.MaxValue.toDouble)
          Histogram.PosInfString
        else outBinArray(i).toString
      outMap(s"$leftString ${Histogram.Separator} $rightString") = outHistArray(i - 1)
    }
    outMap.toMap
  }

  override def toString: String = {
    toGraphString()
  }
}

/**
  * object will transform Histogram into String
  *
  * @note we are keeping it like this just for receiever's unmarshelling can happen without error
  */
object Histogram {
  val Separator = "to"
  val NegInfString = "-inf"
  val PosInfString = "+inf"
  require(!Separator.contains("-"), "Separator cannot contain -")
  require(!Separator.toUpperCase().contains("E"), "Separator cannot contain E")
  require(!Separator.contains("."), "Separator cannot contain . (dot)")

  def apply(map: mutable.LinkedHashMap[String, Double]): Histogram = {

    var listOfBins: mutable.ListBuffer[Double] = ListBuffer[Double]()
    for (keyStr <- map.keys) {
      val splitted = keyStr.split(Separator, 2).toList

      listOfBins += (
        if (splitted.head.trim == NegInfString) Int.MinValue.toDouble
        else splitted.head.toDouble
        )

      listOfBins += (
        if (splitted(1).trim == PosInfString) Int.MaxValue.toDouble
        else splitted(1).toDouble
        )
    }
    val histArray = map.values.toArray
    listOfBins = listOfBins.distinct

    new Histogram(histVector = new DenseVector[Double](histArray), binEdgesVector = new DenseVector[Double](listOfBins.toArray))
  }
}
