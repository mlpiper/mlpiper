package com.parallelmachines.reflex.components.flink.batch.general

import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer

class TwoDup extends FlinkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.flowShaping
  override val label: String = "Duplicate data set"
  override val description: String = "Duplicates data set"
  override val version: String = "1.0.0"

  override lazy val paramInfo: String =
    """[]""".stripMargin

  val input = ComponentConnection(
    tag = typeTag[Any],
    label = "Data",
    description = "Data to duplicate",
    group = ConnectionGroups.DATA)

  val output1 = ComponentConnection(
    tag = typeTag[Any],
    label = "Data1",
    description = "Duplicated data1",
    group = ConnectionGroups.DATA)

  val output2 = ComponentConnection(
    tag = typeTag[Any],
    label = "Data2",
    description = "Duplicated data2",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input)
  override var outputTypes: ConnectionList = ConnectionList(output1, output2)

  @throws(classOf[Exception])
  override def validateAndPropagateIncomingTypes(incomingTypes:ConnectionList) : Unit = {
    validateNumberOfIncoming(incomingTypes)
    // Dup component can accept anything - but once got input infer on the output
    outputTypes = ConnectionList(incomingTypes(0), incomingTypes(0))
  }

  override def configure(paramMap: Map[String, Any]): Unit = {
  }

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {
    val dsInput = dsArr(0).data[DataSet[Any]]
    ArrayBuffer(
      new DataWrapper(dsInput),
      new DataWrapper(dsInput)
    )
  }
}
