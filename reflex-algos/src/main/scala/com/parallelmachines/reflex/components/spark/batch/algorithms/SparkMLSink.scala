package com.parallelmachines.reflex.components.spark.batch.algorithms

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.pipeline.{ComponentConnection, ComponentsGroups, ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

trait SparkMLSink extends SparkBatchComponent with ModelBehavior {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.sinks

  private val sink = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "Dataframe to output",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(sink)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  override val modelBehaviorType: ModelBehaviorType.Value = ModelBehaviorType.Auxiliary


  def doSink(sc: SparkContext, df: DataFrame): Unit

  final override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                                 errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()

    val transformed_df = pipelineInfo.fit().transform(pipelineInfo.dataframe)

    doSink(sc = env, df = transformed_df)

    ArrayBuffer()
  }
}
