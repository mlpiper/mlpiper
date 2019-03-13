package com.parallelmachines.reflex.components.spark.batch.algorithms

import com.parallelmachines.reflex.components.{ComponentAttribute, FeaturesColComponentAttribute, PredictionColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.{PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame


class ReflexKmeansML extends ReflexSparkMLAlgoBase {
  override val label: String = "KMeans Training"
  override lazy val defaultModelName: String = "KMeans"
  override val description: String = "Batch KMeans Training"
  override val version: String = "1.0.0"

  val tempSharedPath = ComponentAttribute("tempSharedPath", "",
    "temp Shared Path", "Temporary shared path for model transfer, " +
      "paths with prefix file:// or hdfs://", optional = true)
  val initMode = ComponentAttribute("initMode", "k-means||", "Initialization Mode", "The initialization algorithm" +
    ". This can be either 'random' to choose random points as initial cluster centers," +
    " or 'k-means||' to use a parallel variant of k-means++. (Default: k-means||).", optional = true)
  initMode.setOptions(List[(String, String)](("random", "random"), ("k-means||", "k-means||")))
  val initStep = ComponentAttribute("initStep", 2, "Initialization Steps", "The number of steps for the" +
    " k-means|| initialization mode. This is an advanced setting -- the default of 2 is almost " +
    "always enough. Must be > 0.", optional = true)
    .setValidator(x => x > 0)
  val k = ComponentAttribute("k", 2, "Number of Clusters/Centers", "Number of Cluster/Centers > 1. (Default: 2)")
    .setValidator(x => x > 1)
  val tol = ComponentAttribute("tol", 1e-5, "Tolerance", "the convergence tolerance for iterative algorithms" +
    ">=0, default 1e-5", optional = true)
    .setValidator(x => x >= 0.0)
  val seed = ComponentAttribute("seed", 1, "Seed", "random seed. (Default: random)", optional = true)
  val maxIter = ComponentAttribute("maxIter", 10, "Maximum Iterations", "maximum number of iterations (>= 0" +
    ") (Default: 10)", optional = true)
    .setValidator(x => x >= 0)

  val featuresCol = FeaturesColComponentAttribute()
  val predictionCol = PredictionColComponentAttribute() //prediction column produced in transform

  attrPack.add(tempSharedPath, seed, maxIter, initMode, initStep, k, tol, featuresCol, predictionCol)

  override def generateModelStat(pipelineModel: PipelineModel,
                                 transformed_df: DataFrame,
                                 sparkContext: SparkContext): Unit = {
    val kmeansMLTrainingStat: KmeansMLTrainingStat = new KmeansMLTrainingStat()

    kmeansMLTrainingStat
      .generateModelStat(pipelineModel = pipelineModel,
        transformed_df = transformed_df,
        sparkContext = sparkContext,
        predictionColName = predictionCol.value)
  }

  override def getAlgoStage(): PipelineStage = {
    val featuresColName = featuresCol.value
    val predictionColName = predictionCol.value
    this.featuresColName = featuresColName

    this.tempSharedPathStr = tempSharedPath.value

    new KMeans()
      .setInitMode(initMode.value)
      .setInitSteps(initStep.value)
      .setK(k.value)
      .setTol(tol.value)
      .setSeed(seed.value)
      .setMaxIter(maxIter.value)
      .setFeaturesCol(featuresColName)
      .setPredictionCol(predictionColName)
  }
}
