package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexNullSourceConnector extends SparkBatchComponent {
  override val isSource = true
  override val group: String = ComponentsGroups.connectors
  override val label = "Null DataFrame"
  override val description = "Stub for producing an empty DataFrame"
  override val version = "1.0.0"

  override lazy val isVisible: Boolean = false

  private val output = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "Data",
    description = "Empty DF",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output)

  def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    val sparkSession = SparkSession.builder().config(env.getConf).getOrCreate()
    //creating SparkBatchPipelineInfo containing empty DF
    val nullSparkBatchPipelineInfo = new SparkBatchPipelineInfo(sparkSession.emptyDataFrame)
    ArrayBuffer(DataWrapper(nullSparkBatchPipelineInfo))
  }
}
