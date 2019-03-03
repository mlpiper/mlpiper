package com.parallelmachines.reflex.components.flink.streaming.general

import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer


class ReflexTwoUnionComponent extends FlinkStreamingComponent {
    override val isSource: Boolean = false
    val group: String = ComponentsGroups.flowShaping
    val label = "Streams Union"
    val description = "Union two streams into one."
    val version = "1.0.0"
    override lazy val paramInfo: String = """[]""".stripMargin

    val input1 = ComponentConnection(
        tag = typeTag[Any],
        label = "Data1",
        description = "Data1 to join",
        group = ConnectionGroups.DATA)

    val input2 = ComponentConnection(
        tag = typeTag[Any],
        label = "Data2",
        description = "Data2 to join",
        group = ConnectionGroups.DATA)

    val output = ComponentConnection(
        tag = typeTag[Any],
        label = "Data",
        description = "Joined data",
        group = ConnectionGroups.DATA)

    val inputTypes: ConnectionList = ConnectionList(input1, input2)
    var outputTypes: ConnectionList = ConnectionList(output)

    override def configure(paramMap: Map[String, Any]): Unit = {
    }

    @throws(classOf[Exception])
    override def validateAndPropagateIncomingTypes(incomingTypes: ConnectionList) : Unit = {
        validateNumberOfIncoming(incomingTypes)

        if (incomingTypes(0).tag != incomingTypes(1).tag) {
            throw new Exception(s"Error: union of 2 different types is not allowed type1: ${incomingTypes(0)} type2: ${incomingTypes(1)}")
        }

        // Dup component can accept anything - but once got input infer on the output
        outputTypes = ConnectionList(incomingTypes(0).tag)
    }

    override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
    ArrayBuffer[DataWrapperBase] = {
        ArrayBuffer[DataWrapperBase]()
    }
}
