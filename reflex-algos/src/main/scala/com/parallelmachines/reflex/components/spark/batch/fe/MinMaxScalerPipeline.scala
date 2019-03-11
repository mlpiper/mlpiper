package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.MinMaxScaler
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class MinMaxScalerPipeline extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Min Max Absolute Scaler"
  override val description: String = "Transforms a Vector, rescaling each feature to a specific range."
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
  val min = ComponentAttribute("min", 0.0, "Lower bound", "Lower bound after transformation. (Default: 0.0).", optional = true)
  val max = ComponentAttribute("max", 1.0, "Upper bound", "Upper bound after transformation. (Default: 1.0).", optional = true)

  attrPack.add(inputCol, outputCol, min, max)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value
    val minValue = min.value
    val maxValue = max.value

    val scaler = new MinMaxScaler()
      .setInputCol(inputColName)
      .setOutputCol(outputColName)
      .setMin(minValue)
      .setMax(maxValue)

    pipelineInfo.addStage(
      stage = scaler,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
