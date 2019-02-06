package com.parallelmachines.reflex.common

import breeze.linalg.DenseVector
import breeze.numerics.pow
import com.parallelmachines.reflex.common.dataanalysis.ContinuousDataAnalyst
import com.parallelmachines.reflex.pipeline.DataFrameUtils
import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexNamedMatrix, ReflexNamedVector}
import org.apache.flink.streaming.scala.examples.clustering.stat.HistogramComparatorTypes
import org.apache.flink.streaming.scala.examples.clustering.stat.continuous.{Histogram => ContinuousHistogram, HistogramWrapper => ContinuousHistogramWrapper, NamedMatrixToFeaturedHistogram => NamedMatrixToFeaturedContinuousHistogram}
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import com.parallelmachines.reflex.common.enums.OpType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class ContinuousHistogramForSpark(healthName: String) extends HealthComponentSpark {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  override def getName(): String = healthName

  override def generateHealth(): Unit = {
    val _ = createHealth()
  }

  override def generateHealthAndCompare(): Unit = {
    /** Inference health generation depends on incoming health.
      * So don't call createHealthAndCompare if incoming health was not set. */
    if (contenderHistogram != null) {
      createHealthAndCompare()
    }
  }

  override def setIncomingHealthStream(input: RDD[String]): Unit = {
    if (!input.isEmpty()) {
      contenderHistogram = ContinuousHistogramWrapper.fromString(input.first())
    }
  }

  override def setContext(sc: SparkContext): Unit = {
    sparkContext = sc
  }

  def setRddOfDenseVector(_rddOfDenseVector: RDD[DenseVector[Double]]): Unit = {
    rddOfDenseVector = _rddOfDenseVector
  }

  def setDfOfDenseVector(_dfOfDenseVector: DataFrame): Unit = {
    dfOfDenseVector = _dfOfDenseVector
  }

  /**
    * createHistogram function is responsible for calculating histogram on RDD of DenseVector
    * Method gives access to named histogram generation on RDD.
    *
    * @return A map containing the featureID and histogram object
    */
  var rddOfDenseVector: RDD[DenseVector[Double]] = _
  var dfOfDenseVector: DataFrame = _
  var sparkMLModel: Option[PipelineModel] = None
  var binSizeForEachFeatureForRef: Option[Map[String, Double]] = None
  var minBinValueForEachFeatureForRef: Option[Map[String, Double]] = None
  var maxBinValueForEachFeatureForRef: Option[Map[String, Double]] = None
  var enableAccumOutputOfHistograms: Boolean = true
  var sparkContext: SparkContext = _

  // TODO: We need to make this variable setter user given
  var enableDataAnalysis: Boolean = true

  /**
    * createHist function is responsible for creating continuous histogram
    * on RDD of DenseVector/NamedVector and DataFrames
    * Method gives access to named histogram generation on RDD.
    *
    * @return A map containing the featureID and histogram object
    */
  def createHealth(): Option[mutable.Map[String, ContinuousHistogram]] = {

    var rddOfNamedVector: RDD[ReflexNamedVector] = null
    var rddOfNamedMatrix: RDD[ReflexNamedMatrix] = null
    var histogram: mutable.Map[String, ContinuousHistogram] = null
    var minBinValueForEachFeature: Option[Map[String, Double]] = null
    var maxBinValueForEachFeature: Option[Map[String, Double]] = null

    // considering all features are continuous, calling up health creation for continuous features
    val genericHealthForContinuousFeatures = new HistogramForContinuousFeatures(healthName)

    require((rddOfDenseVector == null && dfOfDenseVector != null) || (rddOfDenseVector != null && dfOfDenseVector == null))
    require(sparkContext != null)

    if (rddOfDenseVector != null) {
      rddOfNamedVector =
        rddOfDenseVector.map(x => ParsingUtils.denseVectorToReflexNamedVector(x).get)
    } else if (dfOfDenseVector != null) {
      val columnMap =
        if (minBinValueForEachFeatureForRef.isDefined) {
          Some(dfOfDenseVector.columns.map(col =>
            if (maxBinValueForEachFeatureForRef.get.contains(col))
              (col, OpType.CONTINUOUS)
            else (col, OpType.CATEGORICAL)).toMap)
        } else {
          None
        }
      rddOfNamedVector =
        DataFrameUtils.toRDDOfNamedVectorUsingSparkML(df = dfOfDenseVector,
          sparkMLModel = sparkMLModel,
          columnMap = columnMap)
    }

    val rddOfContinuousNamedVector =
      rddOfNamedVector.map(_.toContinuousNamedVector()).filter(_.vector.nonEmpty)

    // generating data analysis for continuous dataset
    if (enableDataAnalysis) {
      val featureAndDAResult = ContinuousDataAnalyst.analyze(rddOfContinuousNamedVector)

      // not outputting continuous DA if result set is empty. This can be case for dataset containing only categorical data
      if (featureAndDAResult.nonEmpty) {
        ContinuousDataAnalyst.updateSparkAccumulatorAndGetResultWrapper(featureAndDA = featureAndDAResult, sparkContext = sparkContext)
      }
    }

    rddOfNamedMatrix = GenericNamedMatrixUtils.createReflexNamedMatrix(rddOfNamedVector = rddOfContinuousNamedVector)

    if (!rddOfNamedMatrix.isEmpty()) {
      val tup =
        ContinuousHistogramForSpark
          .getMinMaxBinRange(rddOfNamedVectorNumerics = rddOfContinuousNamedVector,
            minBinValueForEachFeatureForRef = minBinValueForEachFeatureForRef,
            maxBinValueForEachFeatureForRef = maxBinValueForEachFeatureForRef)
      minBinValueForEachFeature = tup._1
      maxBinValueForEachFeature = tup._2

      histogram =
        genericHealthForContinuousFeatures
          .createHistogram(rddOfNamedMatrix = rddOfNamedMatrix,
            binSizeForEachFeatureForRef = binSizeForEachFeatureForRef,
            minBinValueForEachFeatureForRef = minBinValueForEachFeature,
            maxBinValueForEachFeatureForRef = maxBinValueForEachFeature,
            enableAccumOutputOfHistograms = enableAccumOutputOfHistograms,
            scOption = Some(sparkContext),
            InfoType.InfoType.Health,
            this.getModelId().orNull)

      Some(histogram)
    } else {
      None
    }
  }

  /**
    * Method compares input Histogram Stream with contender Histogram Stream
    *
    * @return Overlap score based on overlap method provided
    */
  var contenderHistogram: mutable.Map[String, ContinuousHistogram] = _
  var overlapType: String = HistogramComparatorTypes.ProbabilityDistribution.toString
  var addAdjustmentNormalizingEdge: Boolean = true

  def createHealthAndCompare(): Unit = {
    require(dfOfDenseVector != null)
    require(contenderHistogram != null)
    require(sparkContext != null)
    require(overlapType != null)

    // updating reference min and max bin ranges of each features
    val (minBinRange, maxBinRange, binSizeOfEachFeature)
    = NamedMatrixToFeaturedContinuousHistogram.generateMinMaxRangeAndBinSizeFromFeaturedHist(contenderHistogram)

    minBinValueForEachFeatureForRef = Some(minBinRange.toMap)
    maxBinValueForEachFeatureForRef = Some(maxBinRange.toMap)
    binSizeForEachFeatureForRef = Some(binSizeOfEachFeature.toMap)

    val inferenceHist = createHealth()

    if (inferenceHist.isDefined) {
      // creating score from histogram comparison
      val genericHealthForContinuousFeatures = new HistogramForContinuousFeatures(healthName)

      val _ = genericHealthForContinuousFeatures
        .compareHistogram(contenderHistogram = contenderHistogram,
          inferenceHist = inferenceHist.get,
          sc = sparkContext,
          method = HistogramComparatorTypes.withName(overlapType),
          addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge,
          modelId = this.getModelId().orNull)

    } else {
      logger.warn(s"For continuous features, inference histogram was not created even though received contender histogram contains " +
        s"${contenderHistogram.keys.mkString(", ")} as features")
    }
  }
}

object MinMaxRangeForHistogramType extends Enumeration {
  type MinMaxRangeType = Value

  val ByCI: MinMaxRangeForHistogramType.MinMaxRangeType = Value("byCI")
  val ByMinMax: MinMaxRangeForHistogramType.MinMaxRangeType = Value("byMinMax")
}

object ContinuousHistogramForSpark {
  private def getMinMaxFromStat(stats: MultivariateStatisticalSummary,
                                minMaxRangeType: MinMaxRangeForHistogramType.MinMaxRangeType)
  : (DenseVector[Double], DenseVector[Double]) = {
    val (minBinDV, maxBinDV) =
      if (minMaxRangeType == MinMaxRangeForHistogramType.ByMinMax) {
        val min: DenseVector[Double] = DenseVector(stats.min.toArray)
        val max: DenseVector[Double] = DenseVector(stats.max.toArray)

        val minBinDV: DenseVector[Double] = min
        val maxBinDV: DenseVector[Double] = max

        (minBinDV, maxBinDV)
      }
      else {
        // keeping max coverage to 2 standard deviation
        val maxStdCoverageFromMean = 2

        val mean: DenseVector[Double] = DenseVector(stats.mean.toArray)

        val variance = stats.variance.toArray

        // generating nonNegativeSquaredVarianceScalar because, some values falls below zero because of precision issue
        val maxNegTolerance = 0.0001
        val nonNegativeSquaredVariance = variance.map(eachSquaredVariance =>
          if (eachSquaredVariance < 0) {
            require(eachSquaredVariance > -maxNegTolerance, s"Squared Variance $eachSquaredVariance Cannot Be Negative Than -$maxNegTolerance")

            0.0
          } else {
            eachSquaredVariance
          }
        )

        val std: DenseVector[Double] = pow(DenseVector(nonNegativeSquaredVariance), 0.5)

        val size = mean.length
        val maxStdCoverageDV = DenseVector.fill[Double](size = size, v = maxStdCoverageFromMean)

        val minBinDV: DenseVector[Double] = mean :- (maxStdCoverageDV :* std)
        val maxBinDV: DenseVector[Double] = mean :+ (maxStdCoverageDV :* std)

        (minBinDV, maxBinDV)
      }

    (minBinDV, maxBinDV)
  }

  /**
    * Method is responsible for setting MinMax range of bin for RDD of DenseVectors.
    */
  def getMinMaxBinRange(rddOfNamedVectorNumerics: RDD[ReflexNamedVector],
                        minBinValueForEachFeatureForRef: Option[Map[String, Double]],
                        maxBinValueForEachFeatureForRef: Option[Map[String, Double]],
                        minMaxRangeType: MinMaxRangeForHistogramType.MinMaxRangeType = MinMaxRangeForHistogramType.ByCI)
  : (Option[Map[String, Double]], Option[Map[String, Double]]) = {
    val (minBinValueForEachFeature, maxBinValueForEachFeature) =
      if (minBinValueForEachFeatureForRef.isDefined && maxBinValueForEachFeatureForRef.isDefined) {
        // Generating histogram in range specified previously
        (minBinValueForEachFeatureForRef, maxBinValueForEachFeatureForRef)
      } else {
        val rddVector = rddOfNamedVectorNumerics
          .map(_.vector.map(_.columnValue.asInstanceOf[Double]))
          .map(x => org.apache.spark.mllib.linalg.Vectors.dense(x))

        val stats = Statistics.colStats(rddVector)

        val (minBinDV, maxBinDV) = getMinMaxFromStat(stats = stats, minMaxRangeType = minMaxRangeType)

        val minBinValueForEachFeature = mutable.Map[String, Double]()
        val maxBinValueForEachFeature = mutable.Map[String, Double]()

        val columnNames = rddOfNamedVectorNumerics.reduce((x, _) => x).vector.map(_.columnName)

        columnNames.zip(minBinDV.toArray)
          .foreach(zippedMinVals => minBinValueForEachFeature(zippedMinVals._1) = zippedMinVals._2)

        columnNames.zip(maxBinDV.toArray)
          .foreach(zippedMaxVals => maxBinValueForEachFeature(zippedMaxVals._1) = zippedMaxVals._2)

        (Some(minBinValueForEachFeature.toMap), Some(maxBinValueForEachFeature.toMap))
      }

    (minBinValueForEachFeature, maxBinValueForEachFeature)
  }
}
