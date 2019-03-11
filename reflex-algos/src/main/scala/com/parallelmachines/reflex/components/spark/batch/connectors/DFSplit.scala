package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import org.apache.spark.SparkContext
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class DFSplit extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.flowShaping
  override val label: String = "Split DataFrame"
  override val description: String = "Splits the DataFrame into two parts using random sampling, according to the ratio provided."
  override val version: String = "1.0.0"

  private val input = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "Input DataFrame",
    group = ConnectionGroups.DATA)

  private val output1 = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame1",
    description = "Part1 of DataFrame",
    group = ConnectionGroups.DATA)

  private val output2 = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame2",
    description = "Part2 of DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output1, output2)

  val splitRatio = ComponentAttribute("splitRatio", 0.8, "Split ratio", "Splits DataFrame into two dataframes [DataFrame1, DataFrame2] where each contains a fraction of the samples [ratio, 1-ratio] respectively.")

  attrPack.add(splitRatio)

  def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val splRatio = splitRatio.value

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()

    val transformed_df = pipelineInfo.fit().transform(pipelineInfo.dataframe)

    val splitDF = transformed_df.randomSplit(Array(splRatio, 1 - splRatio))

    val df1 = splitDF(0)
    val df2 = splitDF(1)

    ArrayBuffer(
      DataWrapper(new SparkBatchPipelineInfo(df1)),
      DataWrapper(new SparkBatchPipelineInfo(df2)))
  }
}
