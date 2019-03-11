package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components._
import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.ChiSqSelector
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ChiSqSelectorComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "ChiSqSelector"
  override val description: String = "ChiSqSelector uses the Chi-Squared test" +
    " to decide which features (categorical) to choose according to their association to the label."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val chiSqSel = new ChiSqSelector()

  val labelCol = LabelColComponentAttribute()
  val featuresCol = FeaturesColComponentAttribute()
  val outputCol = OutputColComponentAttribute()
  val numTopFeatures = ComponentAttribute("numTopFeatures", 50, "Number of Top Features", "Number of " +
    "features that selector will select, ordered by ascending p-value. If the number of features" +
    " is < numTopFeatures, then this will select all features. Must be >=1. (Default: 50)"
    , optional = true).setValidator(x => x >= 1)
  val precentile = ComponentAttribute("precentile", 0.1, "Precentile", "Percentile of features" +
    " that selector will select, ordered by statistics value descending. Only applicable when" +
    " selectorType = 'percentile'.range [0.0, 1.0] (Default: 0.1)", optional = true)
    .setValidator(x => (x <= 1.0) & (x >= 0.0))
  val fpr = ComponentAttribute("fpr", 0.05, "FPR", "The highest p-value for features to be" +
    " kept. Only applicable when selectorType = 'fpr'.range [0.0, 1.0]. (Default: 0.05)", optional = true)
    .setValidator(x => (x <= 1.0) & (x >= 0.0))
  val selectorType = ComponentAttribute("selectorType", "numTopFeatures", "Selector Type", "The selector type of the ChisqSelector. Supported options: 'numTopFeatures', 'percentile', 'fpr'.  (Default: numTopFeatures)", optional = true)
  selectorType.setOptions(List[(String, String)](("numTopFeatures", "numTopFeatures"), ("percentile", "percentile"), ("fpr", "fpr")))


  attrPack.add(featuresCol, labelCol, outputCol, selectorType, numTopFeatures, precentile, fpr)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    if (paramMap.contains(labelCol.key)) {
      chiSqSel.setLabelCol(labelCol.value)
    }
    if (paramMap.contains(featuresCol.key)) {
      chiSqSel.setFeaturesCol(featuresCol.value)
    }
    if (paramMap.contains(numTopFeatures.key)) {
      chiSqSel.setNumTopFeatures(numTopFeatures.value)
    }
    if (paramMap.contains(precentile.key)) {
      chiSqSel.setPercentile(precentile.value)
    }
    if (paramMap.contains(fpr.key)) {
      chiSqSel.setFpr(fpr.value)
    }
    if (paramMap.contains(selectorType.key)) {
      chiSqSel.setSelectorType(selectorType.value)
    }
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val outputColName = outputCol.value

    chiSqSel
      .setOutputCol(outputColName)
    //TODO: Spark 2.2.0 has additional parameters, fdr, fwe

    pipelineInfo.addStage(
      stage = chiSqSel,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
