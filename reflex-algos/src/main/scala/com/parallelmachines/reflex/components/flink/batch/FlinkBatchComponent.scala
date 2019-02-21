package com.parallelmachines.reflex.components.flink.batch

import com.parallelmachines.reflex.pipeline._
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

trait FlinkBatchComponent extends ReflexPipelineComponent {
  override val engineType = ComputeEngineType.FlinkBatch

 override def materialize(envWrapper: EnvWrapperBase, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
   return materialize(envWrapper.env[ExecutionEnvironment], dsArr, errPrefixStr)
  }

  @throws(classOf[Exception])
  override def validateAndPropagateIncomingTypes(incomingTypes:ConnectionList) : Unit = {
    validateNumberOfIncoming(incomingTypes)

    val acceptAllType = ComponentConnection(typeTag[Any])
    // This is a basic full type match check allowing components with input type ANY to get any input type
    for ((incomingType, expectedType) <- incomingTypes zip inputTypes) {
      if (expectedType.tag != acceptAllType.tag && incomingType.tag != expectedType.tag) {
        throw new Exception(s"Error: component $name received type $incomingType while expecting $expectedType")
      }
    }
  }

  def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase]
}
