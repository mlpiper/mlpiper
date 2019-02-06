package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, FeaturesColComponentAttribute, LabelColComponentAttribute}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.RFormula

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class RFormulaComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "RFormula"
  override val description: String = "Selects columns specified by an R model formula." +
    "RFormula produces a vector column of features and a double or string column of label. " +
    "Features are used to predict the label column like in linear regression."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val rFormula = new RFormula()
  val labelCol = LabelColComponentAttribute()
  val featuresCol = FeaturesColComponentAttribute()
  val rForm = ComponentAttribute("RFormula", "", "R Formula", "R formula parameter." +
    " The formula is provided in string form. ")
  val forceIndexLabel = ComponentAttribute("forceIndexLabel", false, "Force Index Label", "Force to " +
    "index label, whether it is numeric or string. (Default: false)", optional = true)

  attrPack.add(featuresCol, labelCol, rForm)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    if (paramMap.contains(labelCol.key)) {
      rFormula.setLabelCol(labelCol.value)
    }
    if (paramMap.contains(forceIndexLabel.key)) {
      rFormula.setForceIndexLabel(forceIndexLabel.value)
    }
    if (paramMap.contains(featuresCol.key)) {
      rFormula.setFeaturesCol(featuresCol.value)
    }
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()

    rFormula.setFormula(rForm.value)

    pipelineInfo.addStage(stage = rFormula)

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
