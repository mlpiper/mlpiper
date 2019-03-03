package org.mlpiper.stat.dataanalysis.continuous

import breeze.linalg.{DenseMatrix, DenseVector}
import com.parallelmachines.reflex.common.InfoType.InfoType
import org.mlpiper.stats.StatNames.ContinuousDataAnalysisResultStat
import org.mlpiper.stats._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.utils.{GenericNamedMatrixUtils, ParsingUtils}

import scala.collection.mutable

/**
  * Object's primary focus will revolve around generating data analysis results for given RDD of named vectors.
  * Please note, we are collecting RDD and it will be stored to driver.
  * Reason is instead of writing huge our logic, I would like to rely on DF's given functionality.
  * Secondly, for certain stats like median, better we use collect instead of going with approx median.
  * adding TODO for that.
  */
object ContinuousDataAnalyst {
  //analyze will generate analysis result for each feature.
  def analyze(rddOfNamedVector: RDD[NamedVector]): mutable.Map[String, ContinuousDataAnalysisResult] = {

    val iteratorRDDOfNamedVector = rddOfNamedVector.collect().toIterator
    val reflexNamedMatrix = GenericNamedMatrixUtils.iteratorOfNamedVectorToNamedMatrix(iteratorRDDOfNamedVector)

    val arrayOfColumnEntry = reflexNamedMatrix.arrayOfVector

    val featureAndDA: mutable.Map[String, ContinuousDataAnalysisResult] = mutable.Map[String, ContinuousDataAnalysisResult]()

    arrayOfColumnEntry.foreach(eachColumnEntry => {
      val featureName = eachColumnEntry.columnName
      val featureValue = eachColumnEntry.columnValue.toArray
      val doubleFeatureValues: Array[Double] =
        featureValue.filter(x => (x != null) && (x != None)).map(_.asInstanceOf[Double]).filter(x => !x.isNaN)

      val sortedArray = doubleFeatureValues.sortWith(_ < _)

      val count = featureValue.length
      val nas: Double = (count - doubleFeatureValues.length) * 100.0 / count
      val zeros = doubleFeatureValues.count(_ == 0.0)

      val min = sortedArray.head
      val max = sortedArray.last
      val median = sortedArray.drop(sortedArray.length / 2).head
      val mean = sortedArray.sum / sortedArray.length

      var variance = 0.0
      var std = 0.0
      if (sortedArray.length > 1) {
        variance = sortedArray.map(x => math.pow(x - mean, 2)).sum * 1.0 / (sortedArray.length - 1)
      }
      if (variance > 0) {
        std = math.sqrt(variance)
      }
      featureAndDA.put(featureName, ContinuousDataAnalysisResult(featureName = featureName,
        count = count,
        NAs = nas.toString + "%",
        zeros = zeros,
        min = min,
        max = max,
        mean = mean,
        median = median,
        std = std
      ))
    })

    featureAndDA
  }

  //updateSparkAccumulatorAndGetResultWrapper will update spark and get result wrappers
  def updateSparkAccumulatorAndGetResultWrapper(featureAndDA: mutable.Map[String, ContinuousDataAnalysisResult],
                                                sparkContext: SparkContext): ContinuousDataAnalysisResultMatrixWrapper = {

    val featuresRow: Array[String] = new Array[String](featureAndDA.size)
    val propertyCols: Array[String] =
      Array("Count",
        "Missing",
        "Zeros",
        "Standard Deviation",
        "Min",
        "Mean",
        "Median",
        "Max")

    val matrix: DenseMatrix[String] = new DenseMatrix[String](rows = featureAndDA.size, cols = propertyCols.length)

    featureAndDA.zipWithIndex.foreach(eachFeatureNameResultAndIndex => {
      // eachFeatureNameResultAndIndex._1._1 == feature name
      // eachFeatureNameResultAndIndex._1._2 == feature ContinuousDataAnalysisResult
      // eachFeatureNameResultAndIndex._2 == index of loop

      featuresRow(eachFeatureNameResultAndIndex._2) = eachFeatureNameResultAndIndex._1._1

      //      "Count",
      //      "Missing",
      //      "Zeros",
      //      "Standard Deviation",
      //      "Min",
      //      "Mean",
      //      "Median",
      //      "Max"
      val arrayOfOrderredValues: Array[String] = Array(eachFeatureNameResultAndIndex._1._2.count,
        eachFeatureNameResultAndIndex._1._2.NAs,
        eachFeatureNameResultAndIndex._1._2.zeros,
        eachFeatureNameResultAndIndex._1._2.std,
        eachFeatureNameResultAndIndex._1._2.min,
        eachFeatureNameResultAndIndex._1._2.mean,
        eachFeatureNameResultAndIndex._1._2.median,
        eachFeatureNameResultAndIndex._1._2.max
      ).map(_.toString())

      matrix(eachFeatureNameResultAndIndex._2, ::) := DenseVector[String](arrayOfOrderredValues).t
    })

    val matrixWrapper: ContinuousDataAnalysisResultMatrixWrapper =
      ContinuousDataAnalysisResultMatrixWrapper(matrix = matrix,
        rowsLabel = featuresRow,
        colsLabel = propertyCols)

    val globalStatForResult =
      StatInfo(
        matrixWrapper.getName,
        StatPolicy.REPLACE,
        StatPolicy.REPLACE
      ).toGlobalStat(
        matrixWrapper,
        accumDataType = AccumData.getGraphType(matrixWrapper.matrix),
        infoType = InfoType.General
      )

    globalStatForResult.updateSparkAccumulator(sparkContext)

    matrixWrapper
  }

}

/**
  * Class holds continuous data analytics result!
  */
case class ContinuousDataAnalysisResult(featureName: String,
                                        count: Int,
                                        NAs: String,
                                        zeros: Int,
                                        min: Double,
                                        max: Double,
                                        mean: Double,
                                        median: Double,
                                        std: Double) extends Serializable {
  override def toString: String = {
    "featureName: " + featureName + "\n" +
      "count: " + count + "\n" +
      "NAs: " + NAs + "\n" +
      "zeros: " + zeros + "\n" +
      "min: " + min + "\n" +
      "max: " + max + "\n" +
      "mean: " + mean + "\n" +
      "median: " + median + "\n" +
      "std: " + std + "\n"
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other: ContinuousDataAnalysisResult =>
        this.featureName == other.featureName &&
          this.count == other.count &&
          this.NAs.substring(0, 2) == other.NAs.substring(0, 2) &&
          this.zeros == other.zeros &&
          this.min == other.min &&
          this.max == other.max &&
          math.abs(this.mean - other.mean) < 0.01 &&
          this.median == other.median &&
          math.abs(this.std - other.std) < 0.01
      case _ =>
        false
    }
  }
}

/**
  * Class holds continuous DA result wrapper which will be used by spark accumulator.
  */
case class ContinuousDataAnalysisResultMatrixWrapper(matrix: DenseMatrix[String],
                                                     rowsLabel: Array[String],
                                                     colsLabel: Array[String])
  extends Serializable {
  def getName: String = {
    ContinuousDataAnalysisResultStat()
  }

  override def toString: String = {
    ParsingUtils.breezeDenseMatrixToJsonMap(matrix, Some(rowsLabel), Some(colsLabel))
  }
}
