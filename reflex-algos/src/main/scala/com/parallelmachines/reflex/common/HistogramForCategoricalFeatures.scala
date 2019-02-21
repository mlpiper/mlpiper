package com.parallelmachines.reflex.common

import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.scala.examples.clustering.math.ReflexNamedMatrix
import org.apache.flink.streaming.scala.examples.clustering.stat.HistogramComparatorTypes
import org.apache.flink.streaming.scala.examples.clustering.stat.categorical.{Histogram => CategoricalHistogram, HistogramWrapper => CategoricalHistogramWrapper, OverlapResult => OverlapResultForCategoricalFeature, _}
import org.apache.flink.streaming.scala.examples.common.stats.GlobalAccumulator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class HistogramForCategoricalFeatures(accumName: String) extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  //////////////////////////////////////////////////////////////////////////////////////
  // Flink - Streaming Applications
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Class is responsible for updating accumulator for featured histogram in flink.
    */
  private class HistogramFlinkAccumulatorUpdater(infoType: InfoType, enableAccumOutputOfHistograms: Boolean = true)
    extends RichMapFunction[mutable.Map[String, CategoricalHistogram], mutable.Map[String, CategoricalHistogram]] {

    private var globalHistStat: GlobalAccumulator[CategoricalHistogramWrapper] = _

    def updateStatAccumulator(hist: mutable.Map[String, CategoricalHistogram]): Unit = {
      if (globalHistStat != null) {
        globalHistStat.localUpdate(CategoricalHistogramWrapper(hist))
      } else {
        globalHistStat = NamedMatrixToFeaturedHistogram.getAccumulator(HealthType.CategoricalHistogramHealth.toString, infoType, null, CategoricalHistogramWrapper(hist))
      }
      globalHistStat.updateFlinkAccumulator(this.getRuntimeContext)
    }

    override def map(value: mutable.Map[String, CategoricalHistogram])
    : mutable.Map[String, CategoricalHistogram] = {
      if (enableAccumOutputOfHistograms) {
        this.updateStatAccumulator(value)
      }

      value
    }
  }

  /**
    * createHistogram function is responsible for calculating histogram on Stream of DenseVector
    *
    * @param streamOfMatrix                DataStream of NamedMatrix
    * @param enableCombining               Parameter to enable/disable combining all histograms generated on parallel subtasks
    * @param enableAccumOutputOfHistograms Parameter to enable/disabling outputting histogram to accumulator
    * @return A map containing the featureID and histogram object
    */
  def createHistogram(streamOfMatrix: DataStream[ReflexNamedMatrix],
                      enableCombining: Boolean,
                      setOfPredefinedCategoriesForFeatures: Option[Map[String, Set[String]]],
                      enableAccumOutputOfHistograms: Boolean,
                      infoType: InfoType)
  : DataStream[mutable.Map[String, CategoricalHistogram]] = {
    // generating histogram for each subtask's namedMatrix
    val eachSubtaskHist: DataStream[mutable.Map[String, CategoricalHistogram]] =
      streamOfMatrix
        .map(new NamedMatrixToFeaturedHistogram())

    val finalHistogramStream: DataStream[mutable.Map[String, CategoricalHistogram]] =
    // combining histograms of all subtask if functionality is enabled
      if (enableCombining) {
        val parallelism = eachSubtaskHist.javaStream.getParallelism

        eachSubtaskHist
          .countWindowAll(parallelism)
          .reduce(new CombineFeaturedHistograms)
      }
      else {
        eachSubtaskHist
      }

    val formattedHistogramStream = finalHistogramStream.map(x => HistogramFormatting.formatFeaturedHistogram(x,
      enableNormalization = true,
      // It is getting called by training. So list of predefined Set is not needed
      setOfPredefinedCategoriesForFeatures = setOfPredefinedCategoriesForFeatures))

    // updating accumulator if it is enabled
    formattedHistogramStream
      .map(new HistogramFlinkAccumulatorUpdater(infoType, enableAccumOutputOfHistograms = enableAccumOutputOfHistograms))
  }

  /**
    * Method compares input stream of DenseVector with contender Histogram Stream
    *
    * @param streamOfMatrixes              DataStream of input named matrix.
    * @param contenderHistStream           DataStream of featured histograms that will be use as reference to compare with input hist.
    * @param enableAccumOutputOfHistograms Flag to enable outputting generated histogram to accumulators
    * @return Overlap score based on overlap method provided
    */
  def createHistogramAndCompare(streamOfMatrixes: DataStream[ReflexNamedMatrix],
                                contenderHistStream: DataStream[mutable.Map[String, CategoricalHistogram]],
                                enableAccumOutputOfHistograms: Boolean,
                                method: HistogramComparatorTypes.HistogramComparatorMethodType,
                                addAdjustmentNormalizingEdge: Boolean,
                                modelId: String)
  : DataStream[OverlapResultForCategoricalFeature] = {
    // broadcasting contender data to all subtask
    val broadcastedContenderHistStream = contenderHistStream.broadcast

    // connecting two inputHistStream and contenderHistStream
    val connectedStreams = broadcastedContenderHistStream.connect(streamOfMatrixes)

    // calculating score
    connectedStreams
      .flatMap(new
          GenerateHistogramAndCalculateOverlap(accumName = accumName,
            modelId = modelId,
            outputGeneratedHistogramToAccum = enableAccumOutputOfHistograms,
            method = method,
            addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge))
      // TODO: Handle multiple nodes flatMap order properly instead of generating histogram on single nodes
      .setParallelism(1)
  }


  //////////////////////////////////////////////////////////////////////////////////////
  // Spark - Batch Applications
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Class is responsible for updating accumulator for featured histogram in Spark.
    */
  private class HistogramSparkAccumulatorUpdater(infoType: InfoType, modelId: String, sc: SparkContext) {

    private var globalHistStat: GlobalAccumulator[CategoricalHistogramWrapper] = _

    def updateStatAccumulator(hist: mutable.Map[String, CategoricalHistogram]): Unit = {
      if (globalHistStat != null) {
        globalHistStat.localUpdate(CategoricalHistogramWrapper(hist))
      } else {
        globalHistStat = NamedMatrixToFeaturedHistogram.getAccumulator(accumName, infoType, modelId, CategoricalHistogramWrapper(hist))
      }
      globalHistStat.updateSparkAccumulator(sc)
    }
  }

  /**
    * createHistogram function is responsible for calculating histogram on RDD of namedVector
    * Method gives access to named histogram generation on RDD.
    * Method will generate histograms for Categorical features.
    * User of API needs to make sure that given NamedMatrix contains only categorical data.
    *
    * @param rddOfNamedMatrix RDD of NamedMatrix
    * @return A map containing the featureID and histogram object
    */
  def createHistogram(rddOfNamedMatrix: RDD[ReflexNamedMatrix],
                      enableAccumOutputOfHistograms: Boolean,
                      setOfPredefinedCategoriesForFeatures: Option[Map[String, Set[String]]],
                      sc: SparkContext,
                      infoType: InfoType,
                      modelId: String)
  : mutable.Map[String, CategoricalHistogram] = {
    val histogram = rddOfNamedMatrix
      // Convert each matrix into a histogram
      .map(x => NamedMatrixToFeaturedHistogram(x))
      // Merge each partition's histogram using a reduce
      .reduce((x, y) => CombineFeaturedHistograms.combineTwoFeaturedHistograms(x, y))

    val finalHistogram = HistogramFormatting.formatFeaturedHistogram(histogram,
      enableNormalization = true,
      setOfPredefinedCategoriesForFeatures = setOfPredefinedCategoriesForFeatures)


    // updating accumulator if it is enabled
    if (enableAccumOutputOfHistograms) {
      new HistogramSparkAccumulatorUpdater(infoType, modelId, sc = sc).updateStatAccumulator(finalHistogram)
    }

    finalHistogram
  }


  def compareHistogram(contenderHistogram: mutable.Map[String, CategoricalHistogram],
                       inferringHist: mutable.Map[String, CategoricalHistogram],
                       method: HistogramComparatorTypes.HistogramComparatorMethodType,
                       addAdjustmentNormalizingEdge: Boolean,
                       sc: SparkContext,
                       modelId: String): OverlapResultForCategoricalFeature = {
    val inputHistogramRepresentation = contenderHistogram
    val contenderHistogramRepresentation = inferringHist

    val score = CompareTwoFeaturedHistograms
      .compare(contenderFeatureHist = contenderHistogram,
        inferringFeatureHist = inferringHist,
        method = method,
        addAdjustmentNormalizingEdge = addAdjustmentNormalizingEdge)

    val overlapResult = new OverlapResultForCategoricalFeature(score = score,
      inputHistStream = inputHistogramRepresentation,
      contenderHistStream = contenderHistogramRepresentation)

    val healthAcc = OverlapResultForCategoricalFeature.getAccumulator(overlapResult, InfoType.InfoType.HealthCompare, modelId)
    healthAcc.updateSparkAccumulator(sc)

    overlapResult
  }
}
