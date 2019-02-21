package org.apache.flink.streaming.scala.examples.clustering.stat.algos.kmeans

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.streaming.scala.examples.common.stats._
import com.parallelmachines.reflex.common.InfoType._

import scala.collection.mutable

/**
  * case class  DistanceMatrixStat holds distance statistic generated from distance matrix.
  * It holds distanceMatrix, sorted list of means and variance's list in same order as mean.
  */
case class DistanceMatrixStat(distanceMatrix: DenseMatrix[Double],
                              var meanVector: Option[DenseVector[Double]] = None,
                              var varianceVector: Option[DenseVector[Double]] = None,
                              var countVector: Option[DenseVector[Double]] = None)
  extends Serializable {
  override def toString: String = {
    val mapOfStat: mutable.Map[String, String] = mutable.Map[String, String](
      "distanceMatrix" -> ParsingUtils.breezeDenseMatrixToJsonMap(distanceMatrix)
    )

    if (meanVector.isDefined) {
      mapOfStat("means") = ParsingUtils.breezeDenseVectorToJsonMap(meanVector.get)
    }

    if (varianceVector.isDefined) {
      mapOfStat("variances") = ParsingUtils.breezeDenseVectorToJsonMap(varianceVector.get)
    }

    if (countVector.isDefined) {
      mapOfStat("count") = ParsingUtils.breezeDenseVectorToJsonMap(countVector.get)
    }

    ParsingUtils.iterableToJSON(mapOfStat)
  }
}

object DistanceMatrixStat {

  def getDistanceMatrixAccumulator(startingDistanceStats: DistanceMatrixStat)
  : GlobalAccumulator[DenseMatrix[Double]] = {
    StatInfo(
      StatNames.DistanceMatrixStat,
      StatPolicy.REPLACE,
      StatPolicy.REPLACE
    ).toGlobalStat(
      startingDistanceStats.distanceMatrix,
      accumDataType = AccumData.getGraphType(startingDistanceStats.distanceMatrix),
      infoType = InfoType.General
    )
  }

  def getMeansAccumulator(startingDistanceStats: DistanceMatrixStat)
  : GlobalAccumulator[DenseVector[Double]] = {
    StatInfo(
      StatNames.DistanceMatrixMeansStat,
      StatPolicy.REPLACE,
      StatPolicy.REPLACE
    ).toGlobalStat(
      startingDistanceStats.meanVector.getOrElse(
        new DenseVector[Double](startingDistanceStats.distanceMatrix.rows)),
      accumDataType = AccumData.getGraphType(
        startingDistanceStats.meanVector.getOrElse(
          new DenseVector[Double](startingDistanceStats.distanceMatrix.rows)
        )),
      infoType =InfoType.General
    )
  }

  def getVariancesAccumulator(startingDistanceStats: DistanceMatrixStat)
  : GlobalAccumulator[DenseVector[Double]] = {
    StatInfo(
      StatNames.DistanceMatrixVariancesStat,
      StatPolicy.REPLACE,
      StatPolicy.REPLACE
    ).toGlobalStat(
      startingDistanceStats.varianceVector.getOrElse(
        new DenseVector[Double](startingDistanceStats.distanceMatrix.rows)),
      accumDataType = AccumData.getGraphType(
        startingDistanceStats.varianceVector.getOrElse(
          new DenseVector[Double](startingDistanceStats.distanceMatrix.rows))),
      infoType = InfoType.General
    )
  }

  def getCountsAccumulator(startingDistanceStats: DistanceMatrixStat)
  : GlobalAccumulator[DenseVector[Double]] = {
    StatInfo(
      StatNames.DistanceMatrixCountStat,
      StatPolicy.REPLACE,
      StatPolicy.REPLACE
    ).toGlobalStat(
      startingDistanceStats.countVector.getOrElse(
        new DenseVector[Double](startingDistanceStats.distanceMatrix.rows)),
      accumDataType = AccumData.getGraphType(
        startingDistanceStats.countVector.getOrElse(
          new DenseVector[Double](startingDistanceStats.distanceMatrix.rows))),
      infoType = InfoType.General
    )
  }
}
