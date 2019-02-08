package org.apache.flink.streaming.scala.examples.common.stats

import org.mlpiper.stat.healthlib.HealthType

object StatNames {

  sealed abstract class StatName(name: String) {
    override def toString: String = name

    def apply(): String = name
  }

  sealed abstract class TableStatName(table: StatTable.Value) extends StatName(table.toString)

  sealed abstract class TableElementStatName(table: StatTable.Value, key: String) extends StatName(s"${table.toString}.$key")

  // Pipeline Stats
  sealed abstract class PipelineStatName(key: String) extends TableElementStatName(StatTable.PIPELINE, key)

  case object Count extends PipelineStatName("count")

  case object SamplesPerSecond extends PipelineStatName("samplesPerSec")

  case object AvePMLatencyMillis extends PipelineStatName("avePMLatencyMillis")

  case object SilhouetteScore extends PipelineStatName("silhouetteScore")

  case object PredictionAccuracy extends PipelineStatName("predictionAccuracy")

  case object ModelOutputTimestamp extends PipelineStatName("modelOutputTimestamp")

  case object ModelsUpdatedCounter extends PipelineStatName("modelUpdated")

  // Classification - Spark Stat
  case object Precision extends PipelineStatName("precision")

  case object WeightedPrecision extends PipelineStatName("weightedPrecision")

  case object Recall extends PipelineStatName("recall")

  case object WeightedRecall extends PipelineStatName("weightedRecall")

  case object F1Score extends PipelineStatName("f1Score")

  case object RecallPerClassPipelineStat extends PipelineStatName("recallPerClass")

  case object PrecisionPerClassPipelineStat extends PipelineStatName("precisionPerClass")

  case object ConfusionMatrixModelStat extends PipelineStatName("confusionMatrix")

  case object predictionConfidenceClassificationStat extends PipelineStatName("predictionConfidence")

  case object meanConfidencePerLabelClassificationStat extends PipelineStatName("meanConfidencePerLabel")

  case object featureImportanceStat extends PipelineStatName("featureImportance")

  //Clustering - Spark Stats
  case object TrainingWSSER extends PipelineStatName("WSSER")

  case object AverageDistanceToCluster extends PipelineStatName("averageDistanceToClusters")

  //Regression - Spark Stats
  case object PredictionRmse extends PipelineStatName("predictionRMSE")

  case object PredictionMse extends PipelineStatName("predictionMSE")

  case object PredictionR2 extends PipelineStatName("predictionR2")

  case object PredictionMae extends PipelineStatName("predictionMAE")

  case object PredictionEv extends PipelineStatName("predictionEV")

  case object PredictionRangeRegressionStat extends PipelineStatName("predictionRange")

  // Table Stats
  case object HeatMap extends TableStatName(StatTable.DATA_HEATMAP)

  case object ContinuousOverlapScore extends StatName(s"${HealthType.ContinuousHistogramHealth.toString}.overlapResult")

  case object CategoricalOverlapScore extends StatName(s"${HealthType.CategoricalHistogramHealth.toString}.overlapResult")

  // Model Stats
  sealed abstract class ModelStatName(key: String) extends TableElementStatName(StatTable.MODEL_STATS, key)

  // for kmeans
  case object DistanceMatrixStat extends ModelStatName("distanceMatrixStat")

  case object DistanceMatrixMeansStat extends ModelStatName("meansStat")

  case object DistanceMatrixVariancesStat extends ModelStatName("variancesStat")

  case object DistanceMatrixCountStat extends ModelStatName("countStat")

  case object PredictionDistanceStat extends ModelStatName("predictionDistance")

  // for svm
  case object SVMModelMarginWidth extends ModelStatName("modelMarginWidth")

  case object AbsoluteMarginDistance extends ModelStatName("absoluteMarginDistance")

  // for pca
  case object ExplainedVariance extends ModelStatName("explainedVariance")

  case object ReconstructionErrorAndThreshold extends ModelStatName("reconstructionErrorAndThreshold")

  // DA
  case object ContinuousDataAnalysisResultStat extends StatName(s"Continuous Data Analysis")

  case object CategoricalDataAnalysisResultStat extends StatName(s"Categorical Data Analysis")

}
