package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import org.apache.spark.SparkContext
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class FromTextFile extends SparkBatchComponent {

  override val isSource: Boolean = true
  override val group: String = ComponentsGroups.connectors
  override val label: String = "File"
  override val description: String = "Reads strings from text file and converts it to Spark's RDD."
  override val version: String = "1.0.0"

  private val output1 = ComponentConnection(
    tag = typeTag[String],
    label = "String",
    description = "Data read from a text file",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output1)

  val filePath = ComponentAttribute("filepath", "", "File Name", "Specify the file path to use as input. " +
    "Valid schemes are 'file:///' and 'hdfs:///'")
  attrPack.add(filePath)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer(new DataWrapper(env.textFile(filePath.value)))
  }
}
