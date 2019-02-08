package org.mlpiper.stat.healthlib

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.common.enums.OpType
import com.parallelmachines.reflex.common.InfoType
import com.parallelmachines.reflex.pipeline.DataFrameUtils
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.stat.dataanalysis.categorical.CategoricalDataAnalyst
import org.mlpiper.stat.histogram.HistogramComparatorTypes
import org.mlpiper.stat.histogram.categorical.{Histogram, HistogramFormatting, HistogramWrapper => CategoricalHistogramWrapper}
import org.mlpiper.utils.{GenericNamedMatrixUtils, ParsingUtils}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class CategoricalHealthForSpark(healthName: String) extends HealthComponentSpark {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  override def getName(): String = healthName

  override def generateHealth(): Unit = {
    val _ = createHealth()
  }

  override def generateHealthAndCompare(): Unit = {
    /** Inference health generation depends on incoming health.
      * So don't call createHealthAndCompare if incoming health was not set.
      */
    if (contenderHistogram != null) {
      val _ = createHealthAndCompare()
    }
  }

  override def setIncomingHealthStream(input: RDD[String]): Unit = {
    if (!input.isEmpty()) {
      contenderHistogram = CategoricalHistogramWrapper.fromString(input.first())
    }
  }

  override def setContext(_sc: SparkContext): Unit = {
    sc = _sc
  }

  override def setRddOfDenseVector(_rddOfDenseVector: RDD[DenseVector[Double]]): Unit = {
    rddOfDenseVector = _rddOfDenseVector
  }

  override def setDfOfDenseVector(_dfOfDenseVector: DataFrame): Unit = {
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
  var enableAccumOutputOfHistograms: Boolean = true
  var sc: SparkContext = _
  var overlapType: String = HistogramComparatorTypes.ProbabilityDistribution.toString
  var addAdjustmentNormalizingEdge: Boolean = true

  var setOfPredefinedCategoriesForFeatures: Option[Map[String, Set[String]]] = None

  // TODO: We need to make this variable setter user given
  var enableDataAnalysis: Boolean = true

  def createHealth(): Option[mutable.Map[String, Histogram]] = {
    require((rddOfDenseVector == null && dfOfDenseVector != null) || (rddOfDenseVector != null && dfOfDenseVector == null))
    require(sc != null)

    // calling up health creation for categorical features
    val genericHealthForCategoricalFeatures = new CategoricalHealth(healthName)

    val rddOfNamedVector: RDD[NamedVector] = if (rddOfDenseVector != null) {
      rddOfDenseVector.map(x => ParsingUtils.denseVectorToNamedVector(x).get)
    } else {
      val columnMap =
        if (setOfPredefinedCategoriesForFeatures.isDefined) {
          Some(dfOfDenseVector.columns.map(col =>
            if (setOfPredefinedCategoriesForFeatures.get.contains(col))
              (col, OpType.CATEGORICAL) else (col, OpType.CONTINUOUS)).toMap)
        } else {
          None
        }
      DataFrameUtils
        .toRDDOfNamedVectorUsingSparkML(df = dfOfDenseVector,
          sparkMLModel = sparkMLModel,
          columnMap = columnMap)
    }

    // generating data analysis for categorical dataset
    if (enableDataAnalysis) {
      val rddOfCategoricalNamedVector = rddOfNamedVector.map(_.toCategoricalNamedVector()).filter(_.vector.nonEmpty)

      val featureAndDAResult = CategoricalDataAnalyst.analyze(rddOfCategoricalNamedVector, sc = sc)

      // not outputting categorical DA if result set is empty. This can be case for dataset containing only continuous data
      if (featureAndDAResult.nonEmpty) {
        CategoricalDataAnalyst.updateSparkAccumulatorAndGetResultWrapper(featureAndDA = featureAndDAResult, sparkContext = sc)
      }
    }

    val rddOfNonNACategoricalNamedVector =
      rddOfNamedVector.map(_.toCategoricalNamedVector(dropNa = true)).filter(_.vector.nonEmpty)

    val rddOfNamedMatrix = GenericNamedMatrixUtils.createReflexNamedMatrix(rddOfNamedVector = rddOfNonNACategoricalNamedVector)

    if (!rddOfNamedMatrix.isEmpty()) {
      val histogram =
        genericHealthForCategoricalFeatures
          .createHistogram(rddOfNamedMatrix = rddOfNamedMatrix,
            enableAccumOutputOfHistograms = enableAccumOutputOfHistograms,
            setOfPredefinedCategoriesForFeatures = setOfPredefinedCategoriesForFeatures,
            sc = sc,
            InfoType.InfoType.Health,
            modelId = this.getModelId().orNull)

      Some(histogram)
    } else {
      None
    }
  }

  var contenderHistogram: mutable.Map[String, Histogram] = _

  /**
    * Method compares input Histogram Stream with contender Histogram Stream
    *
    * @return Overlap score based on overlap method provided
    */
  def createHealthAndCompare(): Unit = {
    require(contenderHistogram != null)
    require(sc != null)

    this.setOfPredefinedCategoriesForFeatures = Some(HistogramFormatting.getFeaturedCategoryMap(contenderHistogram))

    val inferenceHist = createHealth()

    if (inferenceHist.isDefined) {
      // creating score from histogram comparison
      val genericHistogramForCategoricalFeatures = new CategoricalHealth(accumName = this.healthName)

      val _ = genericHistogramForCategoricalFeatures
        .compareHistogram(contenderHistogram = contenderHistogram,
          inferringHist = inferenceHist.get,
          method = HistogramComparatorTypes.withName(overlapType),
          addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge,
          sc = sc,
          modelId = this.getModelId().orNull)
    } else {
      logger.warn(s"For categorical features, inference histogram was not created even though received contender histogram contains " +
        s"${contenderHistogram.keys.mkString(", ")} as features")
    }
  }
}
