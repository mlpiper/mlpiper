package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{InputColComponentAttribute, OutputColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.MaxAbsScaler
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class MaxAbsoluteScalerPipeline extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Max Absolute Scaler"
  override val description: String = "Scale each feature by running maximum absolute value. " +
    "This component will fit and transform each feature individually such that the maximum " +
    "absolute possible value of each feature in the set will be 1.0. It will not shift or " +
    "center the data and will not destroy any sparsity."
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

  attrPack.add(inputCol, outputCol)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value

    val scaler = new MaxAbsScaler()
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    pipelineInfo.addStage(
      stage = scaler,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
