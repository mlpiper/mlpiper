package org.mlpiper.stat.dataanalysis.categorical

import breeze.linalg.{DenseMatrix, DenseVector}
import com.parallelmachines.reflex.common.InfoType.InfoType
import org.apache.flink.streaming.scala.examples.common.stats.StatNames.CategoricalDataAnalysisResultStat
import org.apache.flink.streaming.scala.examples.common.stats.{AccumData, StatInfo, StatPolicy}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.utils.{GenericNamedMatrixUtils, ParsingUtils}

import scala.collection.mutable

/**
  * Object's primary focus will revolve around generating data analysis results for given RDD of named vectors.
  * Please note, we are collecting RDD and it will be stored to driver and stat will be calculated using that!
  * adding TODO for that to use spark's mapper reducer functions.
  */
object CategoricalDataAnalyst {
  //analyze will generate analysis result for each feature.
  def analyze(rddOfNamedVector: RDD[NamedVector],
              sc: SparkContext): mutable.Map[String, CategoricalDataAnalysisResult] = {
    val iteratorRDDOfNamedVector = rddOfNamedVector.collect().toIterator
    val reflexNamedMatrix = GenericNamedMatrixUtils.iteratorOfNamedVectorToNamedMatrix(iteratorRDDOfNamedVector)

    val arrayOfColumnEntry = reflexNamedMatrix.arrayOfVector

    val featureAndDA: mutable.Map[String, CategoricalDataAnalysisResult] = mutable.Map[String, CategoricalDataAnalysisResult]()

    arrayOfColumnEntry.foreach(eachColumnEntry => {

      val featureName = eachColumnEntry.columnName

      val featureValue = eachColumnEntry.columnValue.toArray

      def safeIsNan(x: Any): Boolean = {
        try {
          x.toString.toDouble.isNaN
        } catch {
          case _: Throwable =>
        }
        false
      }

      val stringNoneNAFeatures: Array[String] =
        featureValue.filter(x => x != null && x != None && !safeIsNan(x)).map(_.toString())

      val count = featureValue.length

      val nas: Double = (count - stringNoneNAFeatures.length) * 100.0 / count

      // initialize uniques to zero
      var uniques: Double = 0.0

      // initialize topFreqOccuring to zero and then will be replace if there exists top frequently occurring category
      var topFreqOccuring: Double = 0.0
      var topFreqOccuringCat: String = "N/A"

      // initialize averageStringLength to zero
      var averageStringLength: Double = 0.0

      // creating category related stat only if nonNa category exists
      if (stringNoneNAFeatures.nonEmpty) {
        // creating category and its frequency
        val mapOfCategoryAndFrequency: Map[String, Double] = stringNoneNAFeatures.groupBy(identity).mapValues(_.length)

        // calculating top occuring category and its frequency
        val topCatAndFreq = mapOfCategoryAndFrequency.maxBy(_._2)

        uniques = mapOfCategoryAndFrequency.size
        topFreqOccuring = topCatAndFreq._2
        topFreqOccuringCat = topCatAndFreq._1

        averageStringLength = stringNoneNAFeatures.map(_.length).sum * 1.0 / stringNoneNAFeatures.length
      }
      featureAndDA.put(featureName, CategoricalDataAnalysisResult(featureName = featureName,
        count = count,
        NAs = nas.toString + "%",
        uniques = uniques,
        topFreqOccuring = topFreqOccuring,
        topFreqOccuringCat = topFreqOccuringCat,
        averageStringLength = averageStringLength
      ))
    })


    featureAndDA
  }

  //updateSparkAccumulatorAndGetResultWrapper will update spark and get result wrappers
  def updateSparkAccumulatorAndGetResultWrapper(featureAndDA: mutable.Map[String, CategoricalDataAnalysisResult],
                                                sparkContext: SparkContext): CategoricalDataAnalysisResultMatrixWrapper = {

    val featuresRow: Array[String] = new Array[String](featureAndDA.size)
    val propertyCols: Array[String] =
      Array("Count",
        "Missing",
        "Uniques",
        "Top Frequently Occurring Category",
        "Top Frequency",
        "Average String Length")

    val matrix: DenseMatrix[String] = new DenseMatrix[String](rows = featureAndDA.size, cols = propertyCols.length)

    featureAndDA.zipWithIndex.foreach(eachFeatureNameResultAndIndex => {
      // eachFeatureNameResultAndIndex._1._1 == feature name
      // eachFeatureNameResultAndIndex._1._2 == feature CategoricalDataAnalysisResult
      // eachFeatureNameResultAndIndex._2 == index of loop

      featuresRow(eachFeatureNameResultAndIndex._2) = eachFeatureNameResultAndIndex._1._1

      //      "Count",
      //      "Missing",
      //      "Uniques",
      //      "Top Frequently Occurring Category",
      //      "Top Frequency",
      //      "Average String Length"
      val arrayOfOrderredValues: Array[String] = Array(eachFeatureNameResultAndIndex._1._2.count,
        eachFeatureNameResultAndIndex._1._2.NAs,
        eachFeatureNameResultAndIndex._1._2.uniques,
        eachFeatureNameResultAndIndex._1._2.topFreqOccuringCat,
        eachFeatureNameResultAndIndex._1._2.topFreqOccuring,
        eachFeatureNameResultAndIndex._1._2.averageStringLength
      ).map(_.toString())

      matrix(eachFeatureNameResultAndIndex._2, ::) := DenseVector[String](arrayOfOrderredValues).t
    })

    val matrixWrapper: CategoricalDataAnalysisResultMatrixWrapper =
      CategoricalDataAnalysisResultMatrixWrapper(matrix = matrix,
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
  * Class holds categorical data analytics result!
  */
case class CategoricalDataAnalysisResult(featureName: String,
                                         count: Int,
                                         NAs: String,
                                         uniques: Double,
                                         topFreqOccuring: Double,
                                         topFreqOccuringCat: String,
                                         averageStringLength: Double
                                        ) extends Serializable {
  override def toString: String = {
    "featureName: " + featureName + "\n" +
      "count: " + count + "\n" +
      "NAs: " + NAs + "\n" +
      "uniques: " + uniques + "\n" +
      "topFreqOccuring: " + topFreqOccuring + "\n" +
      "topFreqOccuringCat: " + topFreqOccuringCat + "\n" +
      "averageStringLength: " + averageStringLength + "\n"
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other: CategoricalDataAnalysisResult =>
        this.featureName == other.featureName &&
          this.count == other.count &&
          this.NAs.substring(0, 2) == other.NAs.substring(0, 2) &&
          this.uniques == other.uniques &&
          this.topFreqOccuring == other.topFreqOccuring &&
          this.topFreqOccuringCat == other.topFreqOccuringCat &&
          math.abs(this.averageStringLength - other.averageStringLength) < 0.01
      case _ =>
        false
    }
  }
}

/**
  * Class holds categorical DA result wrapper which will be used by spark accumulator.
  */
case class CategoricalDataAnalysisResultMatrixWrapper(matrix: DenseMatrix[String],
                                                      rowsLabel: Array[String],
                                                      colsLabel: Array[String])
  extends Serializable {
  def getName: String = {
    CategoricalDataAnalysisResultStat()
  }

  override def toString: String = {
    ParsingUtils.breezeDenseMatrixToJsonMap(matrix, Some(rowsLabel), Some(colsLabel))
  }
}
