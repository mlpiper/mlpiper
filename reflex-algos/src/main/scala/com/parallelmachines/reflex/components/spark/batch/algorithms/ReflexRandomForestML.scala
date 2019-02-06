package com.parallelmachines.reflex.components.spark.batch.algorithms

import com.parallelmachines.reflex.components.{ComponentAttribute, FeaturesColComponentAttribute, LabelColComponentAttribute}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.PipelineStage
import scala.reflect.runtime.universe._
import com.parallelmachines.reflex.common.constants.McenterTags

/** This component receives as an input mllib RDD. Then it is convered to a dataframe with
  * a label and a feature vector of spark ML. Then the vector slicer translates it into feature
  * columns.
  * This component should be split in the future to a spark ML component
  * that receives directly a dataframe and a connector component that converts
  * the RDD into the correct dataframe. Thsi component will be generally used for
  * spark ML components.
  * The output of the model is sparkml format
  */

class ReflexRandomForestML extends ReflexSparkMLAlgoBase {
  override val label: String = "Random Forest Classification"
  override val description: String = "Random Forest Classification Training"
  override val version: String = "1.0.0"
  addTags(McenterTags.explainable)

  val tempSharedPath = ComponentAttribute("tempSharedPath", "",
    "temp Shared Path", "Temporary shared path for model transfer, " +
      "paths with prefix file:// or hdfs://", optional = true)
  val significantFeaturesNumber = ComponentAttribute("significantFeaturesNumber", 0,
    "significant Features Number", "Number of significant features in Feature Importance vector. " +
      "0 indicates not presenting feature importance.. (>= 0) (Default: 0)",
    optional = true).setValidator(x => x >= 0)
  val numClasses = ComponentAttribute("num-classes", 2, "Number of Classes", "Number of classes")
  val numTrees = ComponentAttribute("num-trees", 2, "Number of Trees", "Number of Trees")
  val maxDepth = ComponentAttribute("maxDepth", 4, "Max Depth", "Maximum depth of the tree " +
    "(e.g. depth 0 means 1 leaf node, depth 1 means " +
    " 1 internal node + 2 leaf nodes).   " +
    "(suggested value: 4)")

  val maxBins = ComponentAttribute("maxBins", 100, "Max Bins", "Maximum number of bins used for " +
    "splitting features (Default: 100)")
  val impurity = ComponentAttribute("impurity", "gini", "Impurity", "Criterion used for information gain calculation. (Options: 'gini'(default), 'entropy')")
  impurity.setOptions(List[(String, String)](("gini", "gini"), ("entropy", "entropy")))

  // Currently excluded from parameters
  /*
  val categorialFeatureInfo = ComponentAttribute("categorialFeatureInfo", Map.empty[Int, Int],
    "categorialFeatureInfo", "Map storing varity of categorical features. An entry (n to k) " +
      " indicates that feature n is categorical with k categories indexed" +
      " from 0: {0, 1, ..., k-1}.")
      */

  val featureSubsetStrategy = ComponentAttribute("featureSubsetStrategy", "auto",
    "Feature Subset Strategy",
    "Number of features to consider for splits at each node." +
      " Supported values: :auto:, :all:, :sqrt:, :log2:, :onethird:.  If :auto: is" +
      " set, this parameter is set based on numTrees: if numTrees == 1, set to :all:; " +
      " if numTrees is greater than 1 (forest) set to :sqrt:.")
  featureSubsetStrategy.setOptions(List[(String, String)](("auto", "auto"), ("all", "all"), ("sqrt", "sqrt"), ("log2", "log2"), ("onethird", "onethird")))

  val labelCol = LabelColComponentAttribute()
  val featuresCol = FeaturesColComponentAttribute()

  attrPack.add(tempSharedPath, significantFeaturesNumber, numClasses, numTrees, maxDepth, maxBins, impurity,
    featureSubsetStrategy, labelCol, featuresCol)

  override def getLabelColumnName: Option[String] = Some(labelCol.value)

  override def getAlgoStage(): PipelineStage = {
    val labelColName = labelCol.value
    val featuresColName = featuresCol.value
    this.featuresColName = featuresColName
    this.supportFeatureImportance = true
    this.significantFeatures = significantFeaturesNumber.value
    this.tempSharedPathStr = tempSharedPath.value


    new RandomForestClassifier()
      .setMaxDepth(maxDepth.value)
      .setMaxBins(maxBins.value)
      .setImpurity(impurity.value)
      .setNumTrees(numTrees.value)
      .setFeatureSubsetStrategy(featureSubsetStrategy.value)
      .setFeaturesCol(featuresColName)
      .setLabelCol(labelColName)
  }
}