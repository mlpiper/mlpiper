package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class HiveToDF extends SparkBatchComponent {

  override val isSource: Boolean = true
  override val group: String = ComponentsGroups.connectors
  override val label: String = "Hive to DataFrame"
  override val description: String = "Component is responsible for reading from Hive Table & convert it to SparkML DataFrame"
  override val version: String = "1.1.0"

  private val output = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output)

  val schema = ComponentAttribute("schema", "", "Schema", "Enter Hive Schema To Use")
  val query = ComponentAttribute("query", "", "Query", "Enter Query To Extract Tabular Form Dataset")

  attrPack.add(schema, query)

  def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    if (SparkCapability.HiveSupport) {
      val spark: SparkSession =
        SparkSession
          .builder
          .master(env.getConf.get("spark.master"))
          .getOrCreate

      val schemaHive = schema.value
      val queryHive = query.value

      spark.sql(s"USE $schemaHive")

      val df = spark.sql(queryHive)

      val sparkPipelineInfo = new SparkBatchPipelineInfo(df)

      ArrayBuffer(DataWrapper(sparkPipelineInfo))
    }
    else {
      throw new Exception(s"${this.label} component is trying to access Hive. But Hive capability is not enabled!")
    }
  }
}
