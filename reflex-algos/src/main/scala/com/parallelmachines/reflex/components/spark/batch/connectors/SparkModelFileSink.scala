package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.common.SparkMLModel
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline.{ComponentsGroups, ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.mlpiper.sparkutils.SparkMLPipelineModelHelper

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class SparkModelFileSink extends SparkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.sinks
  override val label: String = "Model to file"
  override val description: String = "Save the model to file"
  override val version: String = "1.0.0"

  override lazy val isVisible: Boolean = false

  private val input1 = ComponentConnection(
    tag = typeTag[SparkMLModel],
    label = "Model",
    description = "Model to save to file",
    group = ConnectionGroups.MODEL)

  override val inputTypes: ConnectionList = ConnectionList(input1)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val filePath = ComponentAttribute("modelFileSinkPath", "", "Target file path", "Save this model to the given path")
  attrPack.add(filePath)

  /** Generate the DAG portion of the specific component and the specific engine
    *
    * @param env          Flink environment
    * @param dsArr        Array of DataStream[Any]
    * @param errPrefixStr Error prefix string to use when errors happens during the run of the DAG component
    * @return
    */
  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val model = dsArr(0).data[SparkMLModel]()

    //model.sparkMLModel.save(filePath.value)  //TODO: fix all save for docker and hdfs like pyspark
    val sparkMLPipelineModelHelper = new SparkMLPipelineModelHelper()
    sparkMLPipelineModelHelper.setSharedContext(sparkContext1 = env)
    sparkMLPipelineModelHelper.setLocalPath(filePath.value)
    sparkMLPipelineModelHelper.setSharedPathPrefix(model.tempSharedPathStr)
    sparkMLPipelineModelHelper.saveSparkmlModel(model.sparkMLModel)

    ArrayBuffer[DataWrapperBase]()
  }
}
