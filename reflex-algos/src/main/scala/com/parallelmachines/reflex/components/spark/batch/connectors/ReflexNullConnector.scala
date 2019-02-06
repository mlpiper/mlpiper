package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexNullConnector extends SparkBatchComponent {

  override val isSource = false
  override val group = ComponentsGroups.sinks
  override val label = "Null"
  override val description = "Accepts data and sink it to nothing."
  override val version = "1.0.0"

  override lazy val isVisible: Boolean = false

  private val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to ignore",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer()
  }
}

