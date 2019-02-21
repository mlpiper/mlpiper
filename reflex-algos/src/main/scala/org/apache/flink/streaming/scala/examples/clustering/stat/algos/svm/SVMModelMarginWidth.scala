package org.apache.flink.streaming.scala.examples.clustering.stat.algos.svm

import breeze.linalg.{DenseVector, norm}
import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.streaming.scala.examples.common.stats._

import scala.collection.mutable

case class SVMModelMarginWidth(marginWidth: Double)
  extends Serializable {
  override def toString: String = {
    val mapOfMarginWidth: mutable.Map[String, String] = mutable.Map[String, String](
      "marginWidth" -> marginWidth.toString)

    ParsingUtils.iterableToJSON(mapOfMarginWidth)
  }
}

object SVMModelMarginWidth {

  def getAccumulator(startingMarginWidth: SVMModelMarginWidth)
  : GlobalAccumulator[SVMModelMarginWidth] = {
    StatInfo(
      StatNames.SVMModelMarginWidth,
      StatPolicy.REPLACE,
      StatPolicy.REPLACE)
      .toGlobalStat(
        startingMarginWidth,
        accumDataType = AccumData.getGraphType(startingMarginWidth.marginWidth),
        infoType = InfoType.General
      )
  }

  def calculateMarginWidth(weights: org.apache.spark.mllib.linalg.Vector): SVMModelMarginWidth = {
    val normWeights = org.apache.spark.mllib.linalg.Vectors.norm(weights, 2.0)
    require(normWeights != 0.0, "Model weights can not be zero vector")
    val marginWidth = 2 / normWeights

    SVMModelMarginWidth(marginWidth = marginWidth)
  }

  def calculateMarginWidth(weights: DenseVector[Double]): SVMModelMarginWidth = {
    val normWeights = norm(weights)
    require(normWeights != 0.0, "Model weights can not be zero vector")
    val marginWidth = 2 / normWeights

    SVMModelMarginWidth(marginWidth = marginWidth)
  }

}
