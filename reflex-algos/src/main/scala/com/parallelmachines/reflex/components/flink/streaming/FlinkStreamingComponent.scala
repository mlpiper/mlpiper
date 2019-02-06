package com.parallelmachines.reflex.components.flink.streaming

import com.parallelmachines.reflex.pipeline._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer

trait FlinkStreamingComponent extends ReflexPipelineComponent {
  override val engineType = ComputeEngineType.FlinkStreaming


  @throws(classOf[Exception])
  override def validateAndPropagateIncomingTypes(incomingTypes:ConnectionList) : Unit = {

    validateNumberOfIncoming(incomingTypes)
    if (incomingTypes.length != inputTypes.length) {
      throw new Exception(s"Error: component inputs number is ${inputTypes.length} while received incoming number is ${incomingTypes.length}")
    }

    val acceptAllType = ComponentConnection(typeTag[Any])
    // This is a basic full type match check allowing components with input as ANY to get any output type

    for ((incomingType, expectedType) <- incomingTypes zip inputTypes) {
      if (expectedType.tag != acceptAllType.tag && !incomingType.canConnectTo(expectedType)) {
        throw new Exception(s"Error: component $name received type $incomingType while expecting $expectedType")
      }
    }
  }

  def materialize(envWrapper: EnvWrapperBase, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    return materialize(envWrapper.env[StreamExecutionEnvironment], dsArr, errPrefixStr)
  }

  def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase]
}
