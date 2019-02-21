package com.parallelmachines.reflex.components.spark.batch.algorithms

import com.parallelmachines.reflex.common.InfoType.InfoType
import org.apache.flink.streaming.scala.examples.clustering.stat.algos.kmeans.CentroidToDistanceMatrixStatForSpark
import org.apache.flink.streaming.scala.examples.common.stats._
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Class is responsible for providing training stats related to KMeans Algorithm.
  * Generally, Class shall be accessed by generateModelStat method only
  */
class KmeansMLTrainingStat {
  private val logger = LoggerFactory.getLogger(getClass)

  private var globalStatForWSSER: GlobalAccumulator[Double] = _

  /** Method will update WSSER in associated accumulator */
  private def updateStatAccumulatorForWSSER(wsser: Double, sparkContext: SparkContext): Unit = {
    if (globalStatForWSSER != null) {
      globalStatForWSSER.localUpdate(wsser)
    } else {
      globalStatForWSSER = StatInfo(statName = StatNames.TrainingWSSER,
        localPolicy = StatPolicy.REPLACE,
        globalPolicy = StatPolicy.AVERAGE)
        .toGlobalStat(wsser,
          accumDataType = AccumData.getGraphType(wsser),
          infoType = InfoType.General)

    }
    globalStatForWSSER.updateSparkAccumulator(sparkContext)
  }

  /** Method currently generates following stats
    * 1. Within Set Sum of Squared Errors (WSSER)
    * 2. Inter Cluster Distance
    * 3. Cluster Distribution
    */
  def generateModelStat(pipelineModel: PipelineModel,
                        transformed_df: DataFrame,
                        sparkContext: SparkContext,
                        predictionColName: String): Unit = {
    logger.debug(s"Creating KMeans Training Stat")
    try {
      var kmeansModelOption: Option[KMeansModel] = None

      // looping over all stage to find KMeansModel stage (because, it is not accessible directly from pipelineModel)
      pipelineModel.stages.foreach(
        f = {
          case model: KMeansModel if kmeansModelOption.isEmpty =>
            kmeansModelOption = Some(model)
          case _ =>
        }
      )

      if (kmeansModelOption.isDefined) {
        val kMeansModel: KMeansModel = kmeansModelOption.get

        // calculating wsser
        val computedWSSER = kMeansModel.computeCost(transformed_df)
        this.updateStatAccumulatorForWSSER(wsser = computedWSSER, sparkContext = sparkContext)

        // calculating distance matrix
        val sparkCentroidToDistaneMatrixStat: CentroidToDistanceMatrixStatForSpark =
          new CentroidToDistanceMatrixStatForSpark(sparkContext = sparkContext)
        sparkCentroidToDistaneMatrixStat.generateDistanceMatrixStat(model = kMeansModel)

      }
    } catch {
      case e: Exception =>
        logger.error(s"Model Stat are not generated!\n${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
    }
  }
}
