package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

object SparkCollectedData {
  private var collectedData = mutable.Map[String, Any]()

  def set(key: String, data: Any): Unit = {
    require(!collectedData.contains(key), s"Collected data already contains the key: $key")
    collectedData.put(key, data)
  }

  def get(key: String): Any = {
    require(collectedData.contains(key), s"Collected data does not contain the key: $key")
    collectedData.get(key).get
  }

  def clear: Unit = {
    collectedData.clear
  }

  def isEmpty: Boolean = {
    collectedData.isEmpty
  }
}

class SparkCollectConnector extends SparkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.sinks
  override val label: String = "Spark Collector"
  override val description: String = "Collects a stream and exposes to CollectedData object API"
  override val version: String = "1.0.0"

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to collect",
    group = ConnectionGroups.DATA)

  val inputTypes: ConnectionList = ConnectionList(input)
  var outputTypes: ConnectionList = ConnectionList.empty()

  val resultKey = ComponentAttribute("resultKey", "", "Result Key", "Key to put result")
  attrPack.add(resultKey)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    println("resultKey = " + resultKey.value)
    SparkCollectedData.set(resultKey.value, dsArr(0).data[Any])
    ArrayBuffer[DataWrapperBase]()
  }
}
