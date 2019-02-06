package com.parallelmachines.reflex.components.spark.batch.general

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


class TwoDup extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.flowShaping
  override val label: String = "Duplicate Dataframe"
  override val description: String = "Duplicates Dataframe using Cache"
  override val version: String = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "Data to duplicate",
    group = ConnectionGroups.DATA)

  val output1 = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame1",
    description = "Duplicated data1",
    group = ConnectionGroups.DATA)

  val output2 = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame2",
    description = "Duplicated data2",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output1, output2)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val transformed_df = pipelineInfo.fit().transform(pipelineInfo.dataframe)
    val cachedDF = transformed_df.cache()

    ArrayBuffer(
      DataWrapper(new SparkBatchPipelineInfo(cachedDF)),
      DataWrapper(new SparkBatchPipelineInfo(cachedDF))
    )
  }
}
