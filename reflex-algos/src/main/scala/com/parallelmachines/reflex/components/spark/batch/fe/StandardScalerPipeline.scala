package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StandardScaler

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class StandardScalerPipeline extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Standard Scaler"
  override val description: String = "Standardizes features by removing the mean and scaling to unit variance using column summary statistics on the samples in the data set."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val inputCol = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()
  val withStd = ComponentAttribute("withStd", true, "With Std", "Scales the data to unit standard deviation. (Default: true).", optional = true)
  val withMean = ComponentAttribute("withMean", false, "With Mean", "Centers the data with mean before scaling. It will build a dense output, so take care when applying to sparse input. (Default: false).", optional = true)

  attrPack.add(inputCol, outputCol, withStd, withMean)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value
    val withStdValue = withStd.value
    val withMeanValue = withMean.value

    val scaler = new StandardScaler()
      .setInputCol(inputColName)
      .setOutputCol(outputColName)
      .setWithStd(withStdValue)
      .setWithMean(withMeanValue)

    pipelineInfo.addStage(
      stage = scaler,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
